// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcindexnodeclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

var ClientParams paramtable.GrpcClientConfig

// Client is the grpc client of IndexNode.
type Client struct {
	grpcClient grpcclient.GrpcClient
	addr       string
}

// NewClient creates a new IndexNode client.
func NewClient(ctx context.Context, addr string, encryption bool) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("address is empty")
	}
	ClientParams.InitOnce(typeutil.IndexNodeRole)
	client := &Client{
		addr: addr,
		grpcClient: &grpcclient.ClientBase{
			ClientMaxRecvSize:      ClientParams.ClientMaxRecvSize,
			ClientMaxSendSize:      ClientParams.ClientMaxSendSize,
			DialTimeout:            ClientParams.DialTimeout,
			KeepAliveTime:          ClientParams.KeepAliveTime,
			KeepAliveTimeout:       ClientParams.KeepAliveTimeout,
			RetryServiceNameConfig: "milvus.proto.index.IndexNode",
			MaxAttempts:            ClientParams.MaxAttempts,
			InitialBackoff:         ClientParams.InitialBackoff,
			MaxBackoff:             ClientParams.MaxBackoff,
			BackoffMultiplier:      ClientParams.BackoffMultiplier,
		},
	}
	client.grpcClient.SetRole(typeutil.IndexNodeRole)
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)
	if encryption {
		client.grpcClient.EnableEncryption()
	}
	return client, nil
}

// Init initializes IndexNode's grpc client.
func (c *Client) Init() error {
	return nil
}

// Start starts IndexNode's client service. But it does nothing here.
func (c *Client) Start() error {
	return nil
}

// Stop stops IndexNode's grpc client.
func (c *Client) Stop() error {
	return c.grpcClient.Close()
}

// Register dummy
func (c *Client) Register() error {
	return nil
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) interface{} {
	return indexpb.NewIndexNodeClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

// GetComponentStates gets the component states of IndexNode.
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexNodeClient).GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.ComponentStates), err
}

func (c *Client) GetStatisticsChannel(ctx context.Context) (*milvuspb.StringResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexNodeClient).GetStatisticsChannel(ctx, &internalpb.GetStatisticsChannelRequest{})
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.StringResponse), err
}

// CreateJob sends the build index request to IndexNode.
func (c *Client) CreateJob(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexNodeClient).CreateJob(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// QueryJobs query the task info of the index task.
func (c *Client) QueryJobs(ctx context.Context, req *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexNodeClient).QueryJobs(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*indexpb.QueryJobsResponse), err
}

// DropJobs query the task info of the index task.
func (c *Client) DropJobs(ctx context.Context, req *indexpb.DropJobsRequest) (*commonpb.Status, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexNodeClient).DropJobs(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*commonpb.Status), err
}

// GetJobStats query the task info of the index task.
func (c *Client) GetJobStats(ctx context.Context, req *indexpb.GetJobStatsRequest) (*indexpb.GetJobStatsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexNodeClient).GetJobStats(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*indexpb.GetJobStatsResponse), err
}

// ShowConfigurations gets specified configurations para of IndexNode
func (c *Client) ShowConfigurations(ctx context.Context, req *internalpb.ShowConfigurationsRequest) (*internalpb.ShowConfigurationsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexNodeClient).ShowConfigurations(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}

	return ret.(*internalpb.ShowConfigurationsResponse), err
}

// GetMetrics gets the metrics info of IndexNode.
func (c *Client) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client interface{}) (interface{}, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return client.(indexpb.IndexNodeClient).GetMetrics(ctx, req)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*milvuspb.GetMetricsResponse), err
}
