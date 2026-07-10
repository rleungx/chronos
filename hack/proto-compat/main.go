package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type schema struct {
	messages map[string]*descriptorpb.DescriptorProto
	enums    map[string]*descriptorpb.EnumDescriptorProto
	services map[string]*descriptorpb.ServiceDescriptorProto
}

func load(path string) (schema, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return schema{}, err
	}
	set := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(raw, set); err != nil {
		return schema{}, err
	}
	result := schema{
		messages: map[string]*descriptorpb.DescriptorProto{},
		enums:    map[string]*descriptorpb.EnumDescriptorProto{},
		services: map[string]*descriptorpb.ServiceDescriptorProto{},
	}
	for _, file := range set.File {
		prefix := "." + file.GetPackage()
		for _, message := range file.MessageType {
			indexMessage(result, prefix, message)
		}
		for _, enum := range file.EnumType {
			result.enums[prefix+"."+enum.GetName()] = enum
		}
		for _, service := range file.Service {
			result.services[prefix+"."+service.GetName()] = service
		}
	}
	return result, nil
}

func indexMessage(result schema, prefix string, message *descriptorpb.DescriptorProto) {
	name := prefix + "." + message.GetName()
	result.messages[name] = message
	for _, nested := range message.NestedType {
		indexMessage(result, name, nested)
	}
	for _, enum := range message.EnumType {
		result.enums[name+"."+enum.GetName()] = enum
	}
}

func compareField(message string, oldField, newField *descriptorpb.FieldDescriptorProto) []string {
	var failures []string
	if oldField.GetName() != newField.GetName() {
		failures = append(failures, fmt.Sprintf("%s field %d renamed from %s to %s", message, oldField.GetNumber(), oldField.GetName(), newField.GetName()))
	}
	if oldField.GetType() != newField.GetType() || oldField.GetTypeName() != newField.GetTypeName() {
		failures = append(failures, fmt.Sprintf("%s field %s (%d) changed type", message, oldField.GetName(), oldField.GetNumber()))
	}
	if oldField.GetLabel() != newField.GetLabel() {
		failures = append(failures, fmt.Sprintf("%s field %s (%d) changed cardinality", message, oldField.GetName(), oldField.GetNumber()))
	}
	if oldField.GetProto3Optional() != newField.GetProto3Optional() || (oldField.OneofIndex == nil) != (newField.OneofIndex == nil) || oldField.GetOneofIndex() != newField.GetOneofIndex() {
		failures = append(failures, fmt.Sprintf("%s field %s (%d) changed optional/oneof membership", message, oldField.GetName(), oldField.GetNumber()))
	}
	return failures
}

func compare(oldSchema, newSchema schema) []string {
	var failures []string
	for name, oldMessage := range oldSchema.messages {
		newMessage := newSchema.messages[name]
		if newMessage == nil {
			failures = append(failures, "removed message "+name)
			continue
		}
		newByNumber := map[int32]*descriptorpb.FieldDescriptorProto{}
		newByName := map[string]*descriptorpb.FieldDescriptorProto{}
		for _, field := range newMessage.Field {
			newByNumber[field.GetNumber()] = field
			newByName[field.GetName()] = field
		}
		for _, oldField := range oldMessage.Field {
			newField := newByNumber[oldField.GetNumber()]
			if newField == nil {
				failures = append(failures, fmt.Sprintf("%s removed field %s (%d)", name, oldField.GetName(), oldField.GetNumber()))
				continue
			}
			failures = append(failures, compareField(name, oldField, newField)...)
			if byName := newByName[oldField.GetName()]; byName != nil && byName.GetNumber() != oldField.GetNumber() {
				failures = append(failures, fmt.Sprintf("%s field %s moved from number %d to %d", name, oldField.GetName(), oldField.GetNumber(), byName.GetNumber()))
			}
		}
	}
	for name, oldEnum := range oldSchema.enums {
		newEnum := newSchema.enums[name]
		if newEnum == nil {
			failures = append(failures, "removed enum "+name)
			continue
		}
		values := map[string]int32{}
		for _, value := range newEnum.Value {
			values[value.GetName()] = value.GetNumber()
		}
		for _, oldValue := range oldEnum.Value {
			if number, ok := values[oldValue.GetName()]; !ok || number != oldValue.GetNumber() {
				failures = append(failures, fmt.Sprintf("%s removed or renumbered value %s=%d", name, oldValue.GetName(), oldValue.GetNumber()))
			}
		}
	}
	for name, oldService := range oldSchema.services {
		newService := newSchema.services[name]
		if newService == nil {
			failures = append(failures, "removed service "+name)
			continue
		}
		methods := map[string]*descriptorpb.MethodDescriptorProto{}
		for _, method := range newService.Method {
			methods[method.GetName()] = method
		}
		for _, oldMethod := range oldService.Method {
			method := methods[oldMethod.GetName()]
			if method == nil {
				failures = append(failures, fmt.Sprintf("%s removed method %s", name, oldMethod.GetName()))
				continue
			}
			if oldMethod.GetInputType() != method.GetInputType() || oldMethod.GetOutputType() != method.GetOutputType() || oldMethod.GetClientStreaming() != method.GetClientStreaming() || oldMethod.GetServerStreaming() != method.GetServerStreaming() {
				failures = append(failures, fmt.Sprintf("%s method %s changed its wire signature", name, oldMethod.GetName()))
			}
		}
	}
	sort.Strings(failures)
	return failures
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: proto-compat BASELINE_DESCRIPTOR CURRENT_DESCRIPTOR")
		os.Exit(2)
	}
	oldSchema, err := load(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	newSchema, err := load(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	failures := compare(oldSchema, newSchema)
	if len(failures) > 0 {
		fmt.Fprintln(os.Stderr, "protobuf compatibility check failed:\n- "+strings.Join(failures, "\n- "))
		os.Exit(1)
	}
}
