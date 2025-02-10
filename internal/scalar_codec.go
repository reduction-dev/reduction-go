package internal

import (
	"fmt"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ProtoScalar represents the go types that can be wrapped by protobuf. The
// EncodeScaler and DecodeScaler functions use permissive `any` types so that
// they can work with the caller's types. They will have runtime errors if the
// types don't match ProtoScalar.
type ProtoScalar interface {
	~int | ~int32 | ~int64 |
		~uint | ~uint32 | ~uint64 |
		~float32 | ~float64 |
		~string |
		~bool |
		time.Time
}

var allowedTypes = []string{"int", "int32", "int64", "uint", "uint32", "uint64", "float32", "float64", "string", "bool", "time.Time"}

// EncodeScalar marshals a ProtoScalar value into bytes using protobuf wrappers.
func EncodeScalar[T any](v T) ([]byte, error) {
	switch val := any(v).(type) {
	case int32:
		return proto.Marshal(&wrapperspb.Int32Value{Value: val})
	case int:
		if strconv.IntSize == 32 {
			return proto.Marshal(&wrapperspb.Int32Value{Value: int32(val)})
		}
		return proto.Marshal(&wrapperspb.Int64Value{Value: int64(val)})
	case int64:
		return proto.Marshal(&wrapperspb.Int64Value{Value: val})
	case uint32:
		return proto.Marshal(&wrapperspb.UInt32Value{Value: val})
	case uint:
		if strconv.IntSize == 32 {
			return proto.Marshal(&wrapperspb.UInt32Value{Value: uint32(val)})
		}
		return proto.Marshal(&wrapperspb.UInt64Value{Value: uint64(val)})
	case uint64:
		return proto.Marshal(&wrapperspb.UInt64Value{Value: val})
	case float32:
		return proto.Marshal(&wrapperspb.FloatValue{Value: val})
	case float64:
		return proto.Marshal(&wrapperspb.DoubleValue{Value: val})
	case string:
		return proto.Marshal(&wrapperspb.StringValue{Value: val})
	case bool:
		return proto.Marshal(&wrapperspb.BoolValue{Value: val})
	case time.Time:
		return proto.Marshal(timestamppb.New(val))
	default:
		return nil, fmt.Errorf("unsupported type: %T, allowed types: %v", v, allowedTypes)
	}
}

// DecodeScalar unmarshals bytes into a ProtoScalar value using the appropriate protobuf wrapper.
func DecodeScalar[T any](b []byte) (T, error) {
	var zero T
	switch any(zero).(type) {
	case int32:
		var w wrapperspb.Int32Value
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal int32: %w", err)
		}
		return any(w.Value).(T), nil
	case int:
		if strconv.IntSize == 32 {
			var w wrapperspb.Int32Value
			if err := proto.Unmarshal(b, &w); err != nil {
				return zero, fmt.Errorf("failed to unmarshal int: %w", err)
			}
			return any(int(w.Value)).(T), nil
		}
		var w wrapperspb.Int64Value
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal int: %w", err)
		}
		return any(int(w.Value)).(T), nil
	case int64:
		var w wrapperspb.Int64Value
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal int64: %w", err)
		}
		return any(w.Value).(T), nil
	case uint32:
		var w wrapperspb.UInt32Value
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal uint32: %w", err)
		}
		return any(w.Value).(T), nil
	case uint:
		if strconv.IntSize == 32 {
			var w wrapperspb.UInt32Value
			if err := proto.Unmarshal(b, &w); err != nil {
				return zero, fmt.Errorf("failed to unmarshal uint: %w", err)
			}
			return any(uint(w.Value)).(T), nil
		}
		var w wrapperspb.UInt64Value
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal uint: %w", err)
		}
		return any(uint(w.Value)).(T), nil
	case uint64:
		var w wrapperspb.UInt64Value
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal uint64: %w", err)
		}
		return any(w.Value).(T), nil
	case float32:
		var w wrapperspb.FloatValue
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal float32: %w", err)
		}
		return any(w.Value).(T), nil
	case float64:
		var w wrapperspb.DoubleValue
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal float64: %w", err)
		}
		return any(w.Value).(T), nil
	case string:
		var w wrapperspb.StringValue
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal string: %w", err)
		}
		return any(w.Value).(T), nil
	case bool:
		var w wrapperspb.BoolValue
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal bool: %w", err)
		}
		return any(w.Value).(T), nil
	case time.Time:
		var w timestamppb.Timestamp
		if err := proto.Unmarshal(b, &w); err != nil {
			return zero, fmt.Errorf("failed to unmarshal time.Time: %w", err)
		}
		return any(w.AsTime()).(T), nil
	default:
		return zero, fmt.Errorf("unsupported type: %T, allowed types: %v", zero, allowedTypes)
	}
}
