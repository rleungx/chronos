#!/usr/bin/env bash
set -euo pipefail
python3 - "${1:-}" "${2:-}" <<'PY'
import calendar,datetime,json,re,sys; from pathlib import Path; TOLERANCE_NS=100_000_000
def reject(message): raise ValueError(message)
def fields(path):
    result={}
    for line in path.read_text().splitlines():
        key,sep,value=line.partition("="); result.update({key:value} if sep else {})
    return result
def integer(values,key):
    try: return int(values[key])
    except (KeyError,ValueError): reject(f"invalid {key}")
def timestamp(value):
    match=re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z",value)
    if not match: reject(f"invalid RFC3339 timestamp {value!r}")
    base=datetime.datetime.strptime(match.group(1),"%Y-%m-%dT%H:%M:%S"); return calendar.timegm(base.timetuple())*1_000_000_000+int((match.group(2) or "").ljust(9,"0"))
def load_jsonl(path):
    try: rows=[json.loads(line) for line in path.read_text().splitlines() if line]
    except (OSError,json.JSONDecodeError) as error: reject(f"invalid {path.name}: {error}")
    if not rows: reject(f"empty {path.name}")
    return rows
def stamp(value,label):
    if isinstance(value,list) and len(value)==2 and all(isinstance(item,int) for item in value): return value
    reject(f"invalid {label} stamp")
def log_events(path):
    rows=load_jsonl(path)
    for row in rows: row["_ns"]=timestamp(row.get("timestamp",""))
    return rows
def one(rows,key,value):
    hits=[row for row in rows if str(row.get(key,""))==value]
    if len(hits)==1: return hits[0]
    reject(f"expected one {key}={value}, got {len(hits)}")
def validate(root):
    required="summary.txt degrade-observation.txt pre-fault-probe.log fault-span-trace.jsonl fault-span-bench.log recovery-probe.log recovery-bench.log initial-chronos.log recovery-chronos.log".split()
    for name in required:
        if not (root/name).is_file(): reject(f"missing {name}")
    summary=fields(root/"summary.txt"); observation=fields(root/"degrade-observation.txt")
    pre=fields(root/"pre-fault-probe.log"); post=fields(root/"recovery-probe.log")
    bench=fields(root/"fault-span-bench.log"); smoke=fields(root/"recovery-bench.log")
    if summary.get("result")!="success" or summary.get("evidence_contract_version")!="2":
        reject("summary is not contract-v2 success")
    if summary.get("bench_active_before_fault")!="true" or summary.get("bench_alive_after_recovery_ready")!="true":
        reject("allocator did not span the fault")
    initial=log_events(root/"initial-chronos.log"); recovery=log_events(root/"recovery-chronos.log")
    acquired=one(initial,"event","acquire_succeeded")
    ready=one(initial,"event","ready_state_changed")
    lost=one(initial,"event","keepalive_lost")
    shutdown=one(initial,"shutdown_trigger","identity_lease_lost")
    reacquired=one(recovery,"event","acquire_succeeded")
    reready=one(recovery,"event","ready_state_changed")
    for row in (acquired,ready,lost,shutdown,reacquired,reready):
        if row.get("worker_id")!=summary.get("worker_id") or row.get("instance_id")!=summary.get("instance_id"):
            reject("raw identity mismatch")
    if acquired.get("advertise_endpoint")!=reacquired.get("advertise_endpoint"):
        reject("recovery advertise endpoint changed")
    initial_lease=acquired.get("lease_id"); recovery_lease=reacquired.get("lease_id")
    if not initial_lease or not recovery_lease or initial_lease==recovery_lease:
        reject("recovery must acquire a different lease")
    raw={"initial_identity_acquired_at_unix_ns":acquired["_ns"],"initial_ready_at_unix_ns":ready["_ns"],
         "identity_lease_lost_at_unix_ns":lost["_ns"],"shutdown_triggered_at_unix_ns":shutdown["_ns"],
         "recovery_identity_acquired_at_unix_ns":reacquired["_ns"],"recovery_ready_at_unix_ns":reready["_ns"]}
    for key,value in raw.items():
        if integer(summary,key)!=value: reject(f"summary does not match raw {key}")
    if integer(observation,"identity_lease_lost_at_unix_ns")!=lost["_ns"] or integer(observation,"shutdown_triggered_at_unix_ns")!=shutdown["_ns"]:
        reject("observation does not match raw authority loss")
    order=["initial_identity_acquired_at_unix_ns","initial_ready_at_unix_ns","pre_probe_finished_at_unix_ns",
           "allocator_started_at_unix_ns","fault_injected_at_unix_ns","identity_lease_lost_at_unix_ns",
           "shutdown_triggered_at_unix_ns","authority_barrier_completed_at_unix_ns","restore_started_at_unix_ns",
           "etcd_healthy_at_unix_ns","identity_released_at_unix_ns","recovery_started_at_unix_ns",
           "recovery_identity_acquired_at_unix_ns","recovery_ready_at_unix_ns","allocator_finished_at_unix_ns",
           "post_probe_finished_at_unix_ns","smoke_finished_at_unix_ns"]
    times=[integer(summary,key) for key in order]
    if any(left>right for left,right in zip(times,times[1:])): reject("summary timestamps are out of order")
    if integer(observation,"process_exit_status")!=0 or integer(observation,"process_exit_observed_at_unix_ns")<shutdown["_ns"]:
        reject("initial process did not exit cleanly after shutdown trigger")
    if integer(observation,"readyz_loss_observed_at_unix_ns")<=0 and integer(observation,"process_exit_observed_at_unix_ns")<=0:
        reject("no independent degradation observation")
    timeline=summary.get("timeline_key")
    if not timeline or pre.get("probe_timeline_key")!=timeline or post.get("probe_timeline_key")!=timeline:
        reject("timeline identity changed")
    rows=load_jsonl(root/"fault-span-trace.jsonl")
    setup=[row for row in rows if row.get("record_type")=="setup_attempt"]
    logical=[row for row in rows if row.get("record_type")=="logical_request"]
    terminal=[row for row in rows if row.get("record_type")=="terminal"]
    if [row.get("stage") for row in setup]!=["route_connect","ensure_timeline"] or any(row.get("outcome")!="success" for row in setup):
        reject("setup trace is incomplete")
    if len(terminal)!=1 or rows[-1] is not terminal[0] or terminal[0].get("trace_limit_exhausted") is not False:
        reject("invalid trace terminal")
    if not logical or terminal[0].get("logical_record_count")!=len(logical): reject("logical record count mismatch")
    if [row.get("ordinal") for row in logical]!=list(range(1,len(logical)+1)): reject("logical ordinals are not contiguous")
    failed_attempts=0; successes=[]; failures=[]; previous=None; last_tso=None
    for row in logical:
        if row.get("timeline_key")!=timeline: reject("trace timeline changed")
        started=stamp(row.get("started"),"logical started"); finished=stamp(row.get("finished"),"logical finished")
        if started[0]>finished[0] or started[1]>finished[1]: reject("logical time reversed")
        if abs((finished[0]-started[0])-(finished[1]-started[1]))>TOLERANCE_NS: reject("logical wall/monotonic drift")
        if previous and abs((started[0]-previous[0])-(started[1]-previous[1]))>TOLERANCE_NS: reject("between-record clock jump")
        previous=started
        attempts=row.get("attempts")
        if not isinstance(attempts,list) or not 1<=len(attempts)<=2: reject("invalid attempt count")
        for index,attempt in enumerate(attempts,1):
            if attempt.get("attempt")!=index: reject("attempt ordinal mismatch")
            begin=stamp(attempt.get("started"),"attempt started"); end=stamp(attempt.get("finished"),"attempt finished")
            if not (started[1]<=begin[1]<=end[1]<=finished[1]): reject("attempt outside logical request")
            failed=attempt.get("connect")=="failed" or attempt.get("rpc")=="grpc_error"
            if failed: failed_attempts+=1
            elif attempt.get("rpc")!="success": reject("invalid attempt outcome")
        for refresh in row.get("route_refreshes",[]):
            begin=stamp(refresh.get("started"),"refresh started"); end=stamp(refresh.get("finished"),"refresh finished")
            if refresh.get("outcome") not in ("success","failed") or not (started[1]<=begin[1]<=end[1]<=finished[1]):
                reject("invalid route refresh")
        if row.get("logical_outcome")=="success":
            response=integer(row,"response_received_unix_ns")
            if attempts[-1].get("rpc")!="success" or response!=attempts[-1]["finished"][0]: reject("success timestamp is not response receipt")
            first=integer(row,"range_start"); last=integer(row,"range_end")
            if first>last or (last_tso is not None and first<=last_tso): reject("successful ranges are not monotonic")
            last_tso=last; successes.append(response)
        elif row.get("logical_outcome")=="failure":
            if integer(row,"response_received_unix_ns")!=0 or not any(
                attempt.get("connect")=="failed" or attempt.get("rpc")=="grpc_error" for attempt in attempts):
                reject("invalid logical failure")
            failures.append(finished[0])
        else: reject("invalid logical outcome")
    if terminal[0].get("failed_allocation_attempt_count")!=failed_attempts: reject("terminal attempt count mismatch")
    loss=lost["_ns"]; recovery_ready=reready["_ns"]
    if any(loss<=value<recovery_ready for value in successes): reject("success response inside forbidden interval")
    if not any(loss<=value<recovery_ready for value in failures): reject("no logical failure inside forbidden interval")
    after=[value for value in successes if value>=loss]
    if not after or min(after)<recovery_ready: reject("first post-loss success preceded recovery ready")
    if not any(value<loss for value in successes) or not any(value>=recovery_ready for value in successes):
        reject("trace does not span before and after")
    if integer(bench,"allocate_requests_total")!=len(logical) or integer(bench,"allocate_success_total")!=len(successes):
        reject("bench logical totals do not match trace")
    if integer(bench,"allocate_failed_total")!=len(failures) or integer(bench,"allocate_attempt_failed_total")!=failed_attempts:
        reject("bench failure totals do not match trace")
    if integer(bench,"monotonicity_violations_total")!=0 or bench.get("allocation_timeline_key")!=timeline:
        reject("bench monotonicity/timeline mismatch")
    if integer(pre,"probe_last_tso")>=logical[0].get("range_start",2**64) or last_tso>=integer(post,"probe_first_tso"):
        reject("pre/trace/post ranges are not monotonic")
    if integer(smoke,"allocation_failed_total")!=0 or integer(smoke,"allocation_measured_failed_total")!=0:
        reject("recovery smoke contains failures")
    return True
def self_test():
    import shutil,tempfile
    base=Path(tempfile.mkdtemp()); timeline="bench.fixture.allocate_only.0"; second=1_767_225_600
    ns=lambda offset: second*1_000_000_000+offset
    rfc=lambda offset: datetime.datetime.fromtimestamp(second,tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")+f".{offset:09d}Z"
    write=lambda name,values: (base/name).write_text("".join(f"{key}={value}\n" for key,value in values.items()))
    identity={"worker_id":"worker-chaos","instance_id":"127.0.0.1:50051","advertise_endpoint":"127.0.0.1:50051"}
    initial=[{"timestamp":rfc(0),"event":"acquire_succeeded","lease_id":11,**identity},{"timestamp":rfc(100_000_000),"event":"ready_state_changed",**identity},{"timestamp":rfc(500_000_000),"event":"keepalive_lost",**identity},{"timestamp":rfc(600_000_000),"shutdown_trigger":"identity_lease_lost",**identity}]
    recovery=[{"timestamp":rfc(800_000_000),"event":"acquire_succeeded","lease_id":22,**identity},{"timestamp":rfc(900_000_000),"event":"ready_state_changed",**identity}]
    (base/"initial-chronos.log").write_text("".join(json.dumps(row)+"\n" for row in initial))
    (base/"recovery-chronos.log").write_text("".join(json.dumps(row)+"\n" for row in recovery))
    names="initial_identity_acquired initial_ready pre_probe_finished allocator_started fault_injected identity_lease_lost shutdown_triggered authority_barrier_completed restore_started etcd_healthy identity_released recovery_started recovery_identity_acquired recovery_ready allocator_finished post_probe_finished smoke_finished".split()
    offsets=(0,100_000_000,200_000_000,300_000_000,400_000_000,500_000_000,600_000_000,610_000_000,620_000_000,630_000_000,640_000_000,700_000_000,800_000_000,900_000_000,1_300_000_000,1_400_000_000,1_500_000_000)
    summary={"result":"success","evidence_contract_version":2,"worker_id":identity["worker_id"],"instance_id":identity["instance_id"],"timeline_key":timeline,"bench_active_before_fault":"true","bench_alive_after_recovery_ready":"true"}
    summary.update({f"{name}_at_unix_ns":ns(offset) for name,offset in zip(names,offsets)})
    write("summary.txt",summary)
    write("degrade-observation.txt",{"readyz_loss_observed_at_unix_ns":ns(550_000_000),"process_exit_observed_at_unix_ns":ns(605_000_000),"process_exit_status":0,"identity_lease_lost_at_unix_ns":ns(500_000_000),"shutdown_triggered_at_unix_ns":ns(600_000_000)})
    write("pre-fault-probe.log",{"probe_timeline_key":timeline,"probe_first_tso":99,"probe_last_tso":99})
    write("recovery-probe.log",{"probe_timeline_key":timeline,"probe_first_tso":103,"probe_last_tso":103})
    write("fault-span-bench.log",{"allocate_requests_total":3,"allocate_success_total":2,"allocate_failed_total":1,"allocate_attempt_failed_total":1,"monotonicity_violations_total":0,"allocation_timeline_key":timeline})
    write("recovery-bench.log",{"allocation_failed_total":0,"allocation_measured_failed_total":0})
    setup=lambda stage,elapsed: {"record_type":"setup_attempt","stage":stage,"outcome":"success","started":[ns(250_000_000),elapsed],"finished":[ns(260_000_000),elapsed+10_000_000]}
    logical=lambda ordinal,start,end,outcome,first=0: {"record_type":"logical_request","ordinal":ordinal,"timeline_key":timeline,"started":[ns(start),start],"finished":[ns(end),end],"attempts":[{"attempt":1,"connect":"reused","rpc":"success" if outcome=="success" else "grpc_error","started":[ns(start+10_000_000),start+10_000_000],"finished":[ns(end-10_000_000),end-10_000_000]}],"route_refreshes":[],"logical_outcome":outcome,"response_received_unix_ns":ns(end-10_000_000) if outcome=="success" else 0,**({"range_start":first,"range_end":first} if outcome=="success" else {})}
    trace=[setup("route_connect",1),setup("ensure_timeline",20_000_000),logical(1,300_000_000,350_000_000,"success",100),logical(2,520_000_000,580_000_000,"failure"),logical(3,1_000_000_000,1_100_000_000,"success",102),{"record_type":"terminal","trace_limit_exhausted":False,"logical_record_count":3,"failed_allocation_attempt_count":1}]
    (base/"fault-span-trace.jsonl").write_text("".join(json.dumps(row)+"\n" for row in trace))
    def kv(root,name,key,value):
        values=fields(root/name); values[key]=str(value); (root/name).write_text("".join(f"{k}={v}\n" for k,v in values.items()))
    def row(root,index,key,value,name="fault-span-trace.jsonl"):
        values=load_jsonl(root/name); values[index][key]=value; (root/name).write_text("".join(json.dumps(item)+"\n" for item in values))
    def drop_row(root,index,name):
        values=load_jsonl(root/name); values.pop(index); (root/name).write_text("".join(json.dumps(item)+"\n" for item in values))
    def forbidden(root):
        values=load_jsonl(root/"fault-span-trace.jsonl"); item=values[4]
        item["started"]=[ns(520_000_000),520_000_000]; item["finished"]=[ns(1_000_000_000),1_000_000_000]; item["attempts"][0]["started"]=[ns(530_000_000),530_000_000]; item["attempts"][0]["finished"]=[ns(550_000_000),550_000_000]; item["response_received_unix_ns"]=ns(550_000_000)
        (root/"fault-span-trace.jsonl").write_text("".join(json.dumps(value)+"\n" for value in values))
    def clock_jump(root):
        values=load_jsonl(root/"fault-span-trace.jsonl"); item=values[4]
        for stamp_value in ("started","finished"): item[stamp_value][0]+=200_000_000; item["attempts"][0][stamp_value][0]+=200_000_000
        item["response_received_unix_ns"]+=200_000_000
        (root/"fault-span-trace.jsonl").write_text("".join(json.dumps(value)+"\n" for value in values))
    mutations=[(f"missing-{name}",lambda root,name=name:(root/name).unlink()) for name in
               ("summary.txt","degrade-observation.txt","pre-fault-probe.log","fault-span-trace.jsonl",
                "fault-span-bench.log","recovery-probe.log","recovery-bench.log","initial-chronos.log","recovery-chronos.log")]
    mutations += [
        ("bad-result",lambda root:kv(root,"summary.txt","result","failure")),
        ("bad-contract",lambda root:kv(root,"summary.txt","evidence_contract_version",1)),
        ("inactive-before",lambda root:kv(root,"summary.txt","bench_active_before_fault","false")),
        ("dead-after",lambda root:kv(root,"summary.txt","bench_alive_after_recovery_ready","false")),
        ("raw-time",lambda root:kv(root,"summary.txt","identity_lease_lost_at_unix_ns",1)),
        ("reused-lease",lambda root:row(root,0,"lease_id",11,"recovery-chronos.log")),
        ("missing-loss",lambda root:drop_row(root,2,"initial-chronos.log")),
        ("missing-shutdown",lambda root:drop_row(root,3,"initial-chronos.log")),
        ("bad-order",lambda root:kv(root,"summary.txt","restore_started_at_unix_ns",1)),
        ("unclean-exit",lambda root:kv(root,"degrade-observation.txt","process_exit_status",1)),
        ("no-observer",lambda root:(kv(root,"degrade-observation.txt","readyz_loss_observed_at_unix_ns",0),
                                    kv(root,"degrade-observation.txt","process_exit_observed_at_unix_ns",0))),
        ("timeline",lambda root:kv(root,"pre-fault-probe.log","probe_timeline_key","other")),
        ("setup",lambda root:row(root,0,"outcome","failure")),
        ("terminal",lambda root:row(root,-1,"trace_limit_exhausted",True)),
        ("attempt-count",lambda root:row(root,-1,"failed_allocation_attempt_count",0)),
        ("refresh-accounting",lambda root:row(root,3,"route_refreshes",[{"outcome":"bogus"}])),
        ("ordinal",lambda root:row(root,3,"ordinal",9)),
        ("forbidden-success",forbidden),
        ("between-record-clock-jump",clock_jump),
        ("bench-count",lambda root:kv(root,"fault-span-bench.log","allocate_requests_total",4)),
        ("smoke",lambda root:kv(root,"recovery-bench.log","allocation_failed_total",1))]
    expected={"forbidden-success":"success response inside forbidden interval",
              "between-record-clock-jump":"between-record clock jump"}
    try:
        validate(base)
        for name,mutate in mutations:
            root=base.parent/f"{base.name}-{name}"; shutil.copytree(base,root); mutate(root)
            try: validate(root)
            except ValueError as error:
                if name in expected and expected[name] not in str(error): reject(f"{name} hit {error}")
            else: reject(f"self-test mutation passed: {name}")
            shutil.rmtree(root)
    finally: shutil.rmtree(base)
    print(f"chaos lease-loss verifier self-test PASS ({len(mutations)} negative fixtures)")
try:
    if sys.argv[2]=="--self-test": self_test()
    else:
        if not sys.argv[1]: reject("usage: verify-chaos-lease-loss.sh ARTIFACT_DIR")
        validate(Path(sys.argv[1]))
        print("chaos lease-loss evidence PASS")
except (OSError,ValueError) as error:
    print(f"chaos lease-loss evidence REJECT: {error}",file=sys.stderr)
    sys.exit(1)
PY
