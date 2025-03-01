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

package querynode

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryShardService(t *testing.T) {
	qn, err := genSimpleQueryNode(context.Background())
	require.NoError(t, err)

	qss, err := newQueryShardService(context.Background(), qn.metaReplica, qn.tSafeReplica, qn.ShardClusterService, qn.factory, qn.scheduler)
	assert.NoError(t, err)
	err = qss.addQueryShard(0, "vchan1", 0)
	assert.NoError(t, err)
	found1 := qss.hasQueryShard("vchan1")
	assert.Equal(t, true, found1)
	_, err = qss.getQueryShard("vchan1")
	assert.NoError(t, err)
	err = qss.removeQueryShard("vchan1")
	assert.NoError(t, err)

	found2 := qss.hasQueryShard("vchan2")
	assert.Equal(t, false, found2)
	_, err = qss.getQueryShard("vchan2")
	assert.Error(t, err)
	err = qss.removeQueryShard("vchan2")
	assert.Error(t, err)
}

func TestQueryShardService_InvalidChunkManager(t *testing.T) {
	qn, err := genSimpleQueryNode(context.Background())
	require.NoError(t, err)

	qss, err := newQueryShardService(context.Background(), qn.metaReplica, qn.tSafeReplica, qn.ShardClusterService, qn.factory, qn.scheduler)
	assert.NoError(t, err)

	lcm := qss.localChunkManager
	qss.localChunkManager = nil

	err = qss.addQueryShard(0, "vchan", 0)
	assert.Error(t, err)

	qss.localChunkManager = lcm

	rcm := qss.remoteChunkManager
	qss.remoteChunkManager = nil

	err = qss.addQueryShard(0, "vchan", 0)
	assert.Error(t, err)

	qss.remoteChunkManager = rcm
}
