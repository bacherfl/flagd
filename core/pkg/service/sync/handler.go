package sync

import (
	"buf.build/gen/go/bacherfl/flagd/grpc/go/flagd/sync/v1/syncv1grpc"
	syncv12 "buf.build/gen/go/bacherfl/flagd/protocolbuffers/go/flagd/sync/v1"
	rpc "buf.build/gen/go/open-feature/flagd/grpc/go/sync/v1/syncv1grpc"
	syncv1 "buf.build/gen/go/open-feature/flagd/protocolbuffers/go/sync/v1"
	"context"
	"github.com/open-feature/flagd/core/pkg/logger"
	"github.com/open-feature/flagd/core/pkg/sync"
	syncStore "github.com/open-feature/flagd/core/pkg/sync-store"
)

type newHandler struct {
	syncv1grpc.UnimplementedFlagSyncServiceServer
	oldHandler *handler
}

func (nh *newHandler) SyncFlags(request *syncv12.SyncFlagsRequest, server syncv1grpc.FlagSyncService_SyncFlagsServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errChan := make(chan error)
	dataSync := make(chan sync.DataSync)
	nh.oldHandler.syncStore.RegisterSubscription(ctx, request.GetSelector(), request, dataSync, errChan)
	for {
		select {
		case e := <-errChan:
			return e
		case d := <-dataSync:
			if err := server.Send(&syncv12.SyncFlagsResponse{
				FlagConfiguration: d.FlagData,
				State:             dataSyncToGrpcStateV2(d),
			}); err != nil {
				return err
			}
		case <-server.Context().Done():
			return nil
		}
	}
}

func dataSyncToGrpcStateV2(d sync.DataSync) syncv12.SyncState {
	return syncv12.SyncState(d.Type + 1)
}

func (nh *newHandler) FetchAllFlags(ctx context.Context, request *syncv12.FetchAllFlagsRequest) (*syncv12.FetchAllFlagsResponse, error) {
	data, err := nh.oldHandler.syncStore.FetchAllFlags(ctx, request, request.GetSelector())
	if err != nil {
		return &syncv12.FetchAllFlagsResponse{}, err
	}

	return &syncv12.FetchAllFlagsResponse{
		FlagConfiguration: data.FlagData,
	}, nil
}

type handler struct {
	rpc.UnimplementedFlagSyncServiceServer
	syncStore syncStore.ISyncStore
	logger    *logger.Logger
}

func (l *handler) FetchAllFlags(ctx context.Context, req *syncv1.FetchAllFlagsRequest) (
	*syncv1.FetchAllFlagsResponse,
	error,
) {
	data, err := l.syncStore.FetchAllFlags(ctx, req, req.GetSelector())
	if err != nil {
		return &syncv1.FetchAllFlagsResponse{}, err
	}

	return &syncv1.FetchAllFlagsResponse{
		FlagConfiguration: data.FlagData,
	}, nil
}

func (l *handler) SyncFlags(
	req *syncv1.SyncFlagsRequest,
	stream rpc.FlagSyncService_SyncFlagsServer,
) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errChan := make(chan error)
	dataSync := make(chan sync.DataSync)
	l.syncStore.RegisterSubscription(ctx, req.GetSelector(), req, dataSync, errChan)
	for {
		select {
		case e := <-errChan:
			return e
		case d := <-dataSync:
			if err := stream.Send(&syncv1.SyncFlagsResponse{
				FlagConfiguration: d.FlagData,
				State:             dataSyncToGrpcState(d),
			}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func dataSyncToGrpcState(s sync.DataSync) syncv1.SyncState {
	return syncv1.SyncState(s.Type + 1)
}
