#!/usr/bin/env bash
set -euo pipefail
python3 - "${1:-}" "${2:-}" <<'PY'
import calendar,datetime,json,re,sys; from pathlib import Path; TOLERANCE_NS=100_000_000
def reject(message): raise ValueError(message)
def fields(path):
    return {key:value for line in path.read_text().splitlines() for key,sep,value in [line.partition("=")] if sep}
def integer(values,key):
    try: return int(values[key])
    except (KeyError,ValueError): reject(f"invalid {key}")
def number(values,key):
    try: return float(values[key])
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
    rows=load_jsonl(path); [row.__setitem__("_ns",timestamp(row.get("timestamp",""))) for row in rows]; return rows
def one(rows,key,value):
    hits=[row for row in rows if str(row.get(key,""))==value]
    if len(hits)==1: return hits[0]
    reject(f"expected one {key}={value}, got {len(hits)}")
def validate(root):
    required="summary.txt degrade-observation.txt pre-fault-probe.log fault-span-trace.jsonl fault-span-bench.log recovery-probe.log recovery-bench.log initial-chronos.log recovery-chronos.log".split()
    for name in required: (root/name).is_file() or reject(f"missing {name}")
    summary=fields(root/"summary.txt"); observation=fields(root/"degrade-observation.txt"); pre=fields(root/"pre-fault-probe.log"); post=fields(root/"recovery-probe.log"); bench=fields(root/"fault-span-bench.log"); smoke=fields(root/"recovery-bench.log")
    if summary.get("result")!="success" or summary.get("evidence_contract_version")!="2": reject("summary is not contract-v2 success")
    if summary.get("bench_active_before_fault")!="true" or summary.get("bench_alive_after_recovery_ready")!="true": reject("allocator did not span the fault")
    initial=log_events(root/"initial-chronos.log"); recovery=log_events(root/"recovery-chronos.log")
    preflight=one(initial,"event","preflight_passed"); repreflight=one(recovery,"event","preflight_passed")
    acquired=one(initial,"event","acquire_succeeded"); ready=one(initial,"event","ready_state_changed"); lost=one([row for row in initial if row.get("lease_id") and row.get("result")=="failure" and row.get("worker_id")==summary.get("worker_id") and row.get("instance_id")==summary.get("instance_id")],"event","keepalive_lost")
    shutdown=one(initial,"shutdown_trigger","identity_lease_lost"); reacquired=one(recovery,"event","acquire_succeeded"); reready=one(recovery,"event","ready_state_changed")
    if any(row.get("worker_id")!=summary.get("worker_id") or row.get("instance_id")!=summary.get("instance_id") for row in (acquired,ready,lost,shutdown,reacquired,reready)): reject("raw identity mismatch")
    if acquired.get("advertise_endpoint")!=reacquired.get("advertise_endpoint"): reject("recovery advertise endpoint changed")
    initial_lease=acquired.get("lease_id"); recovery_lease=reacquired.get("lease_id")
    if not initial_lease or lost.get("lease_id")!=initial_lease or not recovery_lease or initial_lease==recovery_lease: reject("loss/recovery lease identity mismatch")
    if not preflight.get("build_commit") or preflight.get("build_commit")!=repreflight.get("build_commit") or summary.get("build_commit")!=preflight.get("build_commit"): reject("runtime build commit mismatch")
    raw={"initial_identity_acquired_at_unix_ns":acquired["_ns"],"initial_ready_at_unix_ns":ready["_ns"],"identity_lease_lost_at_unix_ns":lost["_ns"],"shutdown_triggered_at_unix_ns":shutdown["_ns"],"recovery_identity_acquired_at_unix_ns":reacquired["_ns"],"recovery_ready_at_unix_ns":reready["_ns"]}
    for key,value in raw.items(): integer(summary,key)==value or reject(f"summary does not match raw {key}")
    if integer(observation,"identity_lease_lost_at_unix_ns")!=lost["_ns"] or integer(observation,"shutdown_triggered_at_unix_ns")!=shutdown["_ns"]: reject("observation does not match raw authority loss")
    if not summary.get("identity_key") or observation.get("identity_key")!=summary.get("identity_key") or integer(observation,"identity_release_attempt")<1 or integer(observation,"identity_release_command_status")!=0 or observation.get("identity_release_output")!="" or integer(observation,"identity_released_at_unix_ns")!=integer(summary,"identity_released_at_unix_ns"): reject("invalid identity release observation")
    names="initial_identity_acquired initial_ready pre_probe_finished allocator_started fault_injected identity_lease_lost shutdown_triggered authority_barrier_completed restore_started etcd_healthy identity_released recovery_started recovery_identity_acquired recovery_ready allocator_finished post_probe_finished smoke_finished".split()
    times={name:integer(summary,f"{name}_at_unix_ns") for name in names}
    strict="initial_identity_acquired:initial_ready initial_ready:pre_probe_finished pre_probe_finished:allocator_started allocator_started:fault_injected fault_injected:identity_lease_lost authority_barrier_completed:restore_started etcd_healthy:identity_released identity_released:recovery_started recovery_started:recovery_identity_acquired recovery_ready:allocator_finished allocator_finished:post_probe_finished".split()
    weak="identity_lease_lost:shutdown_triggered shutdown_triggered:authority_barrier_completed restore_started:etcd_healthy recovery_identity_acquired:recovery_ready post_probe_finished:smoke_finished".split()
    if any(times[a]>=times[b] for a,b in (pair.split(":") for pair in strict)) or any(times[a]>times[b] for a,b in (pair.split(":") for pair in weak)): reject("summary timestamps violate contract-v2 order")
    if integer(observation,"process_exit_status")!=0 or integer(observation,"process_exit_observed_at_unix_ns")<shutdown["_ns"]: reject("initial process did not exit cleanly after shutdown trigger")
    timeline=summary.get("timeline_key"); (not timeline or pre.get("probe_timeline_key")!=timeline or post.get("probe_timeline_key")!=timeline) and reject("timeline identity changed")
    rows=load_jsonl(root/"fault-span-trace.jsonl")
    setup=[row for row in rows if row.get("record_type")=="setup_attempt"]; logical=[row for row in rows if row.get("record_type")=="logical_request"]; terminal=[row for row in rows if row.get("record_type")=="terminal"]
    if [row.get("stage") for row in setup]!=["route_connect","ensure_timeline"] or any(row.get("outcome")!="success" for row in setup): reject("setup trace is incomplete")
    if len(terminal)!=1 or rows[-1] is not terminal[0] or terminal[0].get("trace_limit_exhausted") is not False: reject("invalid trace terminal")
    if not logical or terminal[0].get("logical_record_count")!=len(logical): reject("logical record count mismatch")
    if [row.get("ordinal") for row in logical]!=list(range(1,len(logical)+1)): reject("logical ordinals are not contiguous")
    for row in setup: begin=stamp(row.get("started"),"setup started"); end=stamp(row.get("finished"),"setup finished"); (begin[0]>end[0] or begin[1]>end[1] or abs((end[0]-begin[0])-(end[1]-begin[1]))>TOLERANCE_NS) and reject("invalid setup time")
    terminal_finished=stamp(terminal[0].get("finished"),"terminal finished")
    if integer(summary,"allocator_started_at_unix_ns")>setup[0]["started"][0] or integer(summary,"allocator_finished_at_unix_ns")<terminal_finished[0]: reject("allocator summary does not contain trace")
    failed_attempts=0; retries=0; refresh_count=0; successes=[]; failures=[]; previous=None; previous_finished=None; first_tso=None; last_tso=None
    for row in logical:
        if row.get("timeline_key")!=timeline: reject("trace timeline changed")
        started=stamp(row.get("started"),"logical started"); finished=stamp(row.get("finished"),"logical finished")
        if started[0]>finished[0] or started[1]>finished[1]: reject("logical time reversed")
        if abs((finished[0]-started[0])-(finished[1]-started[1]))>TOLERANCE_NS: reject("logical wall/monotonic drift")
        if previous and abs((started[0]-previous[0])-(started[1]-previous[1]))>TOLERANCE_NS: reject("between-record clock jump")
        if previous_finished and (previous_finished[0]>started[0] or previous_finished[1]>started[1]): reject("logical rows overlap")
        previous=started; previous_finished=finished
        attempts=row.get("attempts")
        if not isinstance(attempts,list) or not 1<=len(attempts)<=2: reject("invalid attempt count")
        retries+=len(attempts)-1; prior_attempt=None; attempt_times=[]
        for index,attempt in enumerate(attempts,1):
            if attempt.get("attempt")!=index: reject("attempt ordinal mismatch")
            begin=stamp(attempt.get("started"),"attempt started"); end=stamp(attempt.get("finished"),"attempt finished")
            if not (started[0]<=begin[0]<=end[0]<=finished[0] and started[1]<=begin[1]<=end[1]<=finished[1]): reject("attempt outside logical request")
            if abs((end[0]-begin[0])-(end[1]-begin[1]))>TOLERANCE_NS: reject("attempt wall/monotonic drift")
            if prior_attempt and (prior_attempt[0]>begin[0] or prior_attempt[1]>begin[1]): reject("attempts overlap")
            prior_attempt=end; attempt_times.append((begin,end))
            connect=attempt.get("connect"); rpc=attempt.get("rpc")
            if (connect=="failed")!=(rpc=="not_run") or connect not in ("failed","connected","reused") or rpc not in ("not_run","grpc_error","success"): reject("contradictory attempt outcome")
            if (rpc=="grpc_error")!=(isinstance(attempt.get("grpc_code"),str) and bool(attempt["grpc_code"])) or (rpc!="grpc_error" and "grpc_code" in attempt): reject("invalid grpc_code evidence")
            if rpc=="success" and index!=len(attempts): reject("success before later attempt")
            if connect=="failed" or rpc=="grpc_error": failed_attempts+=1
        refreshes=row.get("route_refreshes")
        if not isinstance(refreshes,list) or len(refreshes)>1 or (refreshes and attempts[0].get("rpc")!="grpc_error"): reject("invalid refresh cardinality/trigger")
        for refresh in refreshes:
            begin=stamp(refresh.get("started"),"refresh started"); end=stamp(refresh.get("finished"),"refresh finished")
            if refresh.get("outcome") not in ("success","failed") or not (started[0]<=begin[0]<=end[0]<=finished[0] and started[1]<=begin[1]<=end[1]<=finished[1]) or abs((end[0]-begin[0])-(end[1]-begin[1]))>TOLERANCE_NS: reject("invalid route refresh")
            if attempt_times[0][1][0]>begin[0] or attempt_times[0][1][1]>begin[1]: reject("route refresh precedes first grpc failure")
            if refresh["outcome"]=="failed" and (len(attempts)!=1 or row.get("logical_outcome")!="failure"): reject("failed refresh must end logical request")
            if refresh["outcome"]=="success" and (len(attempts)!=2 or end[0]>attempt_times[1][0][0] or end[1]>attempt_times[1][0][1]): reject("successful refresh must precede retry")
            refresh_count+=1
        if row.get("logical_outcome")=="success":
            response=integer(row,"response_received_unix_ns")
            if attempts[-1].get("rpc")!="success" or response!=attempts[-1]["finished"][0]: reject("success timestamp is not response receipt")
            first=integer(row,"range_start"); last=integer(row,"range_end")
            if first>last or (last_tso is not None and first<=last_tso): reject("successful ranges are not monotonic")
            first_tso=first if first_tso is None else first_tso; last_tso=last; successes.append(response)
        elif row.get("logical_outcome")=="failure":
            if integer(row,"response_received_unix_ns")!=0 or attempts[-1].get("rpc")=="success": reject("invalid logical failure")
            failures.append(attempts[-1]["finished"][0])
        else: reject("invalid logical outcome")
    last_finished=stamp(logical[-1].get("finished"),"last logical finished")
    if terminal_finished[0]<last_finished[0] or terminal_finished[1]<last_finished[1] or abs((terminal_finished[0]-last_finished[0])-(terminal_finished[1]-last_finished[1]))>TOLERANCE_NS: reject("invalid terminal time")
    expected_terminal={"logical_success_count":len(successes),"logical_failure_count":len(failures),"failed_allocation_attempt_count":failed_attempts,"retry_count":retries,"route_refresh_count":refresh_count}
    if any(terminal[0].get(key)!=value for key,value in expected_terminal.items()): reject("terminal counters mismatch")
    loss=lost["_ns"]; recovery_ready=reready["_ns"]
    if any(loss<=value<recovery_ready for value in successes): reject("success response inside forbidden interval")
    if not any(loss<=value<recovery_ready for value in failures): reject("no logical failure inside forbidden interval")
    after=[value for value in successes if value>=loss]
    if not after or min(after)<recovery_ready: reject("first post-loss success preceded recovery ready")
    if not any(value<loss for value in successes) or not any(value>=recovery_ready for value in successes): reject("trace does not span before and after")
    if integer(bench,"allocate_requests_total")!=len(logical) or integer(bench,"allocate_success_total")!=len(successes): reject("bench logical totals do not match trace")
    if integer(bench,"allocate_failed_total")!=len(failures) or integer(bench,"allocate_attempt_failed_total")!=failed_attempts: reject("bench failure totals do not match trace")
    if integer(bench,"monotonicity_violations_total")!=0 or bench.get("allocation_timeline_key")!=timeline: reject("bench monotonicity/timeline mismatch")
    if first_tso is None or integer(pre,"probe_last_tso")>=first_tso or last_tso>=integer(post,"probe_first_tso"): reject("pre/trace/post ranges are not monotonic")
    if integer(smoke,"allocation_failed_total")!=0 or integer(smoke,"allocation_measured_failed_total")!=0: reject("recovery smoke contains failures")
    if number(smoke,"req_per_sec")<10 or integer(smoke,"latency_p95_us")>500000 or integer(smoke,"latency_p999_us")>1000000: reject("recovery smoke threshold failed")
def self_test():
    import shutil,tempfile; base=Path(tempfile.mkdtemp()); timeline="bench.fixture.allocate_only.0"; second=1_767_225_600
    ns=lambda offset: second*1_000_000_000+offset
    rfc=lambda offset: datetime.datetime.fromtimestamp(second,tz=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")+f".{offset:09d}Z"
    write=lambda name,values: (base/name).write_text("".join(f"{key}={value}\n" for key,value in values.items()))
    identity={"worker_id":"worker-chaos","instance_id":"127.0.0.1:50051","advertise_endpoint":"127.0.0.1:50051"}
    initial=[{"timestamp":rfc(0),"event":"acquire_succeeded","lease_id":11,**identity},{"timestamp":rfc(100_000_000),"event":"ready_state_changed",**identity},{"timestamp":rfc(500_000_000),"event":"keepalive_lost","result":"failure","lease_id":11,**identity},{"timestamp":rfc(510_000_000),"event":"keepalive_lost","result":"failure","action":"shutdown",**identity},{"timestamp":rfc(600_000_000),"shutdown_trigger":"identity_lease_lost",**identity},{"timestamp":rfc(0),"event":"preflight_passed","build_commit":"abc",**identity}]
    recovery=[{"timestamp":rfc(800_000_000),"event":"acquire_succeeded","lease_id":22,**identity},{"timestamp":rfc(900_000_000),"event":"ready_state_changed",**identity},{"timestamp":rfc(700_000_000),"event":"preflight_passed","build_commit":"abc",**identity}]
    (base/"initial-chronos.log").write_text("".join(json.dumps(row)+"\n" for row in initial)); (base/"recovery-chronos.log").write_text("".join(json.dumps(row)+"\n" for row in recovery))
    names="initial_identity_acquired initial_ready pre_probe_finished allocator_started fault_injected identity_lease_lost shutdown_triggered authority_barrier_completed restore_started etcd_healthy identity_released recovery_started recovery_identity_acquired recovery_ready allocator_finished post_probe_finished smoke_finished".split()
    offsets=(0,100_000_000,200_000_000,300_000_000,400_000_000,500_000_000,600_000_000,610_000_000,620_000_000,630_000_000,640_000_000,700_000_000,800_000_000,900_000_000,1_300_000_000,1_400_000_000,1_500_000_000)
    summary={"result":"success","evidence_contract_version":2,"worker_id":identity["worker_id"],"instance_id":identity["instance_id"],"timeline_key":timeline,"build_commit":"abc","identity_key":"/fixture/identity","bench_active_before_fault":"true","bench_alive_after_recovery_ready":"true"}
    summary.update({f"{name}_at_unix_ns":ns(offset) for name,offset in zip(names,offsets)}); write("summary.txt",summary)
    write("degrade-observation.txt",{"process_exit_observed_at_unix_ns":ns(605_000_000),"process_exit_status":0,"identity_lease_lost_at_unix_ns":ns(500_000_000),"shutdown_triggered_at_unix_ns":ns(600_000_000),"identity_key":"/fixture/identity","identity_release_attempt":2,"identity_release_command_status":0,"identity_release_output":"","identity_released_at_unix_ns":ns(640_000_000)})
    write("pre-fault-probe.log",{"probe_timeline_key":timeline,"probe_first_tso":99,"probe_last_tso":99})
    write("recovery-probe.log",{"probe_timeline_key":timeline,"probe_first_tso":103,"probe_last_tso":103})
    write("fault-span-bench.log",{"allocate_requests_total":3,"allocate_success_total":2,"allocate_failed_total":1,"allocate_attempt_failed_total":1,"monotonicity_violations_total":0,"allocation_timeline_key":timeline})
    write("recovery-bench.log",{"allocation_failed_total":0,"allocation_measured_failed_total":0,"req_per_sec":10,"latency_p95_us":500000,"latency_p999_us":1000000})
    setup=lambda stage,elapsed: {"record_type":"setup_attempt","stage":stage,"outcome":"success","started":[ns(310_000_000),elapsed],"finished":[ns(320_000_000),elapsed+10_000_000]}
    logical=lambda ordinal,start,end,outcome,first=0: {"record_type":"logical_request","ordinal":ordinal,"timeline_key":timeline,"started":[ns(start),start],"finished":[ns(end),end],"attempts":[{"attempt":1,"connect":"reused","rpc":"success" if outcome=="success" else "grpc_error","started":[ns(start+10_000_000),start+10_000_000],"finished":[ns(end-10_000_000),end-10_000_000],**({"grpc_code":"Unavailable"} if outcome!="success" else {})}],"route_refreshes":[],"logical_outcome":outcome,"response_received_unix_ns":ns(end-10_000_000) if outcome=="success" else 0,**({"range_start":first,"range_end":first} if outcome=="success" else {})}
    trace=[setup("route_connect",1),setup("ensure_timeline",20_000_000),logical(1,330_000_000,450_000_000,"success",100),logical(2,520_000_000,800_000_000,"failure"),logical(3,1_000_000_000,1_100_000_000,"success",102),{"record_type":"terminal","trace_limit_exhausted":False,"logical_record_count":3,"logical_success_count":2,"logical_failure_count":1,"failed_allocation_attempt_count":1,"retry_count":0,"route_refresh_count":0,"finished":[ns(1_200_000_000),1_200_000_000]}]
    trace[2]["attempts"][0]["connect"]="connected"; (base/"fault-span-trace.jsonl").write_text("".join(json.dumps(row)+"\n" for row in trace))
    def save(root,name,values): (root/name).write_text("".join(json.dumps(item)+"\n" for item in values))
    def kv(root,name,key,value): values=fields(root/name); values[key]=str(value); (root/name).write_text("".join(f"{k}={v}\n" for k,v in values.items()))
    def row(root,index,key,value,name="fault-span-trace.jsonl"): values=load_jsonl(root/name); values[index][key]=value; save(root,name,values)
    def drop_row(root,index,name): values=load_jsonl(root/name); values.pop(index); save(root,name,values)
    def attempt(root,row_index,key,value): values=load_jsonl(root/"fault-span-trace.jsonl"); values[row_index]["attempts"][0][key]=value; save(root,"fault-span-trace.jsonl",values)
    def forbidden(root):
        values=load_jsonl(root/"fault-span-trace.jsonl"); item=values[4]; item["started"]=[ns(810_000_000),810_000_000]; item["finished"]=[ns(1_000_000_000),1_000_000_000]; item["attempts"][0]["started"]=[ns(820_000_000),820_000_000]; item["attempts"][0]["finished"]=[ns(850_000_000),850_000_000]; item["response_received_unix_ns"]=ns(850_000_000); save(root,"fault-span-trace.jsonl",values)
    def clock_jump(root):
        values=load_jsonl(root/"fault-span-trace.jsonl"); item=values[4]
        for stamp_value in ("started","finished"): item[stamp_value][0]+=200_000_000; item["attempts"][0][stamp_value][0]+=200_000_000
        item["response_received_unix_ns"]+=200_000_000; save(root,"fault-span-trace.jsonl",values)
    def delayed_failure(root):
        values=load_jsonl(root/"fault-span-trace.jsonl"); item=values[3]; item["started"]=[ns(460_000_000),460_000_000]; item["attempts"][0]["started"]=[ns(470_000_000),470_000_000]; item["attempts"][0]["finished"]=[ns(490_000_000),490_000_000]; save(root,"fault-span-trace.jsonl",values)
    def first_failure(root):
        values=load_jsonl(root/"fault-span-trace.jsonl"); item=values[2]; inside=json.loads(json.dumps(values[3])); item.update({"logical_outcome":"failure","response_received_unix_ns":0}); item["attempts"][0].update({"rpc":"grpc_error","grpc_code":"Unavailable"}); item.pop("range_start"); item.pop("range_end")
        values[3]=logical(2,460_000_000,490_000_000,"success",100); inside["ordinal"]=3; values.insert(4,inside); values[5]["ordinal"]=4; values[-1].update({"logical_record_count":4,"logical_success_count":2,"logical_failure_count":2,"failed_allocation_attempt_count":2}); save(root,"fault-span-trace.jsonl",values)
        kv(root,"fault-span-bench.log","allocate_requests_total",4); kv(root,"fault-span-bench.log","allocate_failed_total",2); kv(root,"fault-span-bench.log","allocate_attempt_failed_total",2); kv(root,"pre-fault-probe.log","probe_last_tso",101)
    def success_before_later(root):
        values=load_jsonl(root/"fault-span-trace.jsonl"); item=values[2]; item["attempts"].append({"attempt":2,"connect":"reused","rpc":"grpc_error","grpc_code":"Unavailable","started":[ns(445_000_000),445_000_000],"finished":[ns(448_000_000),448_000_000]}); save(root,"fault-span-trace.jsonl",values)
    def refresh_case(root,failed_retry=False):
        values=load_jsonl(root/"fault-span-trace.jsonl"); item=values[3]; item["route_refreshes"]=[{"outcome":"failed","started":[ns(600_000_000),600_000_000],"finished":[ns(610_000_000),610_000_000]}]; values[-1]["route_refresh_count"]=1
        if failed_retry:
            item["attempts"][0]["finished"]=[ns(580_000_000),580_000_000]; item["attempts"].append({"attempt":2,"connect":"reused","rpc":"grpc_error","grpc_code":"Unavailable","started":[ns(620_000_000),620_000_000],"finished":[ns(700_000_000),700_000_000]}); values[-1].update({"retry_count":1,"failed_allocation_attempt_count":2}); kv(root,"fault-span-bench.log","allocate_attempt_failed_total",2)
        save(root,"fault-span-trace.jsonl",values)
    mutations=[
        ("raw-time",lambda root:kv(root,"summary.txt","identity_lease_lost_at_unix_ns",1)), ("reused-lease",lambda root:row(root,0,"lease_id",11,"recovery-chronos.log")), ("missing-loss",lambda root:drop_row(root,2,"initial-chronos.log")), ("forged-loss",lambda root:row(root,2,"lease_id","","initial-chronos.log")), ("missing-shutdown",lambda root:drop_row(root,4,"initial-chronos.log")),
        ("bad-order",lambda root:kv(root,"summary.txt","restore_started_at_unix_ns",1)), ("strict-equality",lambda root:kv(root,"summary.txt","fault_injected_at_unix_ns",ns(500_000_000))), ("identity-status",lambda root:kv(root,"degrade-observation.txt","identity_release_command_status",1)), ("build-identity",lambda root:row(root,2,"build_commit","def","recovery-chronos.log")),
        ("missing-process-observer",lambda root:kv(root,"degrade-observation.txt","process_exit_observed_at_unix_ns",0)), ("timeline",lambda root:kv(root,"pre-fault-probe.log","probe_timeline_key","other")), ("setup",lambda root:row(root,0,"outcome","failure")), ("allocator-start-binding",lambda root:kv(root,"summary.txt","allocator_started_at_unix_ns",ns(400_000_000))), ("allocator-finish-binding",lambda root:row(root,-1,"finished",[ns(1_400_000_000),1_400_000_000])),
        ("terminal",lambda root:row(root,-1,"trace_limit_exhausted",True)), ("attempt-count",lambda root:row(root,-1,"failed_allocation_attempt_count",0)), ("logical-count",lambda root:row(root,-1,"logical_success_count",0)), ("refresh-accounting",lambda root:row(root,3,"route_refreshes",[{"outcome":"bogus"}])), ("grpc-code",lambda root:attempt(root,3,"grpc_code","")), ("connect-rpc-contradiction",lambda root:attempt(root,2,"connect","failed")), ("unknown-connect",lambda root:attempt(root,2,"connect","mystery")),
        ("failure-ending-success",lambda root:attempt(root,3,"rpc","success")), ("attempt-clock-drift",lambda root:attempt(root,3,"started",[ns(650_000_000),530_000_000])), ("success-before-later",success_before_later), ("ordinal",lambda root:row(root,3,"ordinal",9)), ("refresh-out-of-order",lambda root:refresh_case(root)), ("failed-refresh-retry",lambda root:refresh_case(root,True)), ("forbidden-success",forbidden), ("delayed-failure",delayed_failure),
        ("first-success-range",first_failure), ("between-record-clock-jump",clock_jump), ("bench-count",lambda root:kv(root,"fault-span-bench.log","allocate_requests_total",4)), ("smoke",lambda root:kv(root,"recovery-bench.log","allocation_failed_total",1)), ("smoke-throughput",lambda root:kv(root,"recovery-bench.log","req_per_sec",9)),
        ("smoke-latency",lambda root:kv(root,"recovery-bench.log","latency_p95_us",500001))]
    expected={"forbidden-success":"success response inside forbidden interval","between-record-clock-jump":"between-record clock jump","delayed-failure":"no logical failure inside forbidden interval","first-success-range":"pre/trace/post ranges are not monotonic","refresh-out-of-order":"route refresh precedes first grpc failure","failed-refresh-retry":"failed refresh must end logical request"}
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
        validate(Path(sys.argv[1])); print("chaos lease-loss evidence PASS")
except (OSError,ValueError) as error:
    print(f"chaos lease-loss evidence REJECT: {error}",file=sys.stderr); sys.exit(1)
PY
