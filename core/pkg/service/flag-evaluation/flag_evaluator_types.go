package service

import (
	evalV1 "buf.build/gen/go/bacherfl/flagd/protocolbuffers/go/flagd/evaluation/v1"
	connect2 "connectrpc.com/connect"
	"fmt"

	schemaV1 "buf.build/gen/go/open-feature/flagd/protocolbuffers/go/schema/v1"
	"github.com/bufbuild/connect-go"
	"google.golang.org/protobuf/types/known/structpb"
)

type response[T constraints] interface {
	SetResult(value T, variant, reason string) error
}

type constraints interface {
	bool | string | map[string]any | float64 | int64
}

type booleanResponse struct {
	schemaV1Resp *connect.Response[schemaV1.ResolveBooleanResponse]
	evalV1Resp   *connect2.Response[evalV1.ResolveBooleanResponse]
}

func (r *booleanResponse) SetResult(value bool, variant, reason string) error {
	if r.schemaV1Resp != nil {
		r.schemaV1Resp.Msg.Value = value
		r.schemaV1Resp.Msg.Variant = variant
		r.schemaV1Resp.Msg.Reason = reason
	}
	if r.evalV1Resp != nil {
		r.evalV1Resp.Msg.Value = value
		r.evalV1Resp.Msg.Variant = variant
		r.evalV1Resp.Msg.Reason = reason
	}

	return nil
}

type stringResponse struct {
	schemaV1Resp *connect.Response[schemaV1.ResolveStringResponse]
	evalV1Resp   *connect2.Response[evalV1.ResolveStringResponse]
}

func (r *stringResponse) SetResult(value, variant, reason string) error {
	if r.schemaV1Resp != nil {
		r.schemaV1Resp.Msg.Value = value
		r.schemaV1Resp.Msg.Variant = variant
		r.schemaV1Resp.Msg.Reason = reason
	}
	if r.evalV1Resp != nil {
		r.evalV1Resp.Msg.Value = value
		r.evalV1Resp.Msg.Variant = variant
		r.evalV1Resp.Msg.Reason = reason
	}
	return nil
}

type floatResponse struct {
	schemaV1Resp *connect.Response[schemaV1.ResolveFloatResponse]
	evalV1Resp   *connect2.Response[evalV1.ResolveFloatResponse]
}

func (r *floatResponse) SetResult(value float64, variant, reason string) error {
	if r.schemaV1Resp != nil {
		r.schemaV1Resp.Msg.Value = value
		r.schemaV1Resp.Msg.Variant = variant
		r.schemaV1Resp.Msg.Reason = reason
	}
	if r.evalV1Resp != nil {
		r.evalV1Resp.Msg.Value = value
		r.evalV1Resp.Msg.Variant = variant
		r.evalV1Resp.Msg.Reason = reason
	}
	return nil
}

type intResponse struct {
	schemaV1Resp *connect.Response[schemaV1.ResolveIntResponse]
	evalV1Resp   *connect2.Response[evalV1.ResolveIntResponse]
}

func (r *intResponse) SetResult(value int64, variant, reason string) error {
	if r.schemaV1Resp != nil {
		r.schemaV1Resp.Msg.Value = value
		r.schemaV1Resp.Msg.Variant = variant
		r.schemaV1Resp.Msg.Reason = reason
	}
	if r.evalV1Resp != nil {
		r.evalV1Resp.Msg.Value = value
		r.evalV1Resp.Msg.Variant = variant
		r.evalV1Resp.Msg.Reason = reason
	}
	return nil
}

type objectResponse struct {
	schemaV1Resp *connect.Response[schemaV1.ResolveObjectResponse]
	evalV1Resp   *connect2.Response[evalV1.ResolveObjectResponse]
}

func (r *objectResponse) SetResult(value map[string]any, variant, reason string) error {
	if r.schemaV1Resp != nil {
		r.schemaV1Resp.Msg.Reason = reason
		val, err := structpb.NewStruct(value)
		if err != nil {
			return fmt.Errorf("struct response construction: %w", err)
		}

		r.schemaV1Resp.Msg.Value = val
		r.schemaV1Resp.Msg.Variant = variant
	}
	if r.evalV1Resp != nil {
		r.evalV1Resp.Msg.Reason = reason
		val, err := structpb.NewStruct(value)
		if err != nil {
			return fmt.Errorf("struct response construction: %w", err)
		}

		r.evalV1Resp.Msg.Value = val
		r.evalV1Resp.Msg.Variant = variant
	}
	return nil
}
