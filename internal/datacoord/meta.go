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

// Package datacoord contains core functions in datacoord
package datacoord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/common"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

type meta struct {
	sync.RWMutex
	ctx         context.Context
	catalog     metastore.DataCoordCatalog
	collections map[UniqueID]*collectionInfo // collection id to collection info
	segments    *SegmentsInfo                // segment id to segment info
}

type collectionInfo struct {
	ID             int64
	Schema         *schemapb.CollectionSchema
	Partitions     []int64
	StartPositions []*commonpb.KeyDataPair
	Properties     map[string]string
}

// NewMeta creates meta from provided `kv.TxnKV`
func newMeta(ctx context.Context, kv kv.TxnKV, chunkManagerRootPath string) (*meta, error) {
	mt := &meta{
		ctx:         ctx,
		catalog:     &datacoord.Catalog{Txn: kv, ChunkManagerRootPath: chunkManagerRootPath},
		collections: make(map[UniqueID]*collectionInfo),
		segments:    NewSegmentsInfo(),
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

// reloadFromKV loads meta from KV storage
func (m *meta) reloadFromKV() error {
	segments, err := m.catalog.ListSegments(m.ctx)
	if err != nil {
		return err
	}
	metrics.DataCoordNumCollections.WithLabelValues().Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.SealedSegmentLabel).Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.GrowingSegmentLabel).Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.FlushedSegmentLabel).Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.FlushingSegmentLabel).Set(0)
	metrics.DataCoordNumSegments.WithLabelValues(metrics.DropedSegmentLabel).Set(0)
	metrics.DataCoordNumStoredRows.WithLabelValues().Set(0)
	numStoredRows := int64(0)
	for _, segment := range segments {
		m.segments.SetSegment(segment.ID, NewSegmentInfo(segment))
		metrics.DataCoordNumSegments.WithLabelValues(segment.State.String()).Inc()
		if segment.State == commonpb.SegmentState_Flushed {
			numStoredRows += segment.NumOfRows
		}
	}
	metrics.DataCoordNumStoredRows.WithLabelValues().Set(float64(numStoredRows))
	metrics.DataCoordNumStoredRowsCounter.WithLabelValues().Add(float64(numStoredRows))
	return nil
}

// AddCollection adds a collection into meta
// Note that collection info is just for caching and will not be set into etcd from datacoord
func (m *meta) AddCollection(collection *collectionInfo) {
	log.Debug("meta update: add collection",
		zap.Int64("collection ID", collection.ID))
	m.Lock()
	defer m.Unlock()
	m.collections[collection.ID] = collection
	metrics.DataCoordNumCollections.WithLabelValues().Set(float64(len(m.collections)))
	log.Debug("meta update: add collection - complete",
		zap.Int64("collection ID", collection.ID))
}

// GetCollection returns collection info with provided collection id from local cache
func (m *meta) GetCollection(collectionID UniqueID) *collectionInfo {
	m.RLock()
	defer m.RUnlock()
	collection, ok := m.collections[collectionID]
	if !ok {
		return nil
	}
	return collection
}

func (m *meta) GetClonedCollectionInfo(collectionID UniqueID) *collectionInfo {
	m.RLock()
	defer m.RUnlock()

	coll, ok := m.collections[collectionID]
	if !ok {
		return nil
	}

	clonedProperties := make(map[string]string)
	maps.Copy(clonedProperties, coll.Properties)
	cloneColl := &collectionInfo{
		ID:             coll.ID,
		Schema:         proto.Clone(coll.Schema).(*schemapb.CollectionSchema),
		Partitions:     coll.Partitions,
		StartPositions: common.CloneKeyDataPairs(coll.StartPositions),
		Properties:     clonedProperties,
	}

	return cloneColl
}

// chanPartSegments is an internal result struct, which is aggregates of SegmentInfos with same collectionID, partitionID and channelName
type chanPartSegments struct {
	collectionID UniqueID
	partitionID  UniqueID
	channelName  string
	segments     []*SegmentInfo
}

// GetSegmentsChanPart returns segments organized in Channel-Partition dimension with selector applied
func (m *meta) GetSegmentsChanPart(selector SegmentInfoSelector) []*chanPartSegments {
	m.RLock()
	defer m.RUnlock()
	mDimEntry := make(map[string]*chanPartSegments)

	for _, segmentInfo := range m.segments.segments {
		if !selector(segmentInfo) {
			continue
		}
		dim := fmt.Sprintf("%d-%s", segmentInfo.PartitionID, segmentInfo.InsertChannel)
		entry, ok := mDimEntry[dim]
		if !ok {
			entry = &chanPartSegments{
				collectionID: segmentInfo.CollectionID,
				partitionID:  segmentInfo.PartitionID,
				channelName:  segmentInfo.InsertChannel,
			}
			mDimEntry[dim] = entry
		}
		entry.segments = append(entry.segments, segmentInfo)
	}

	result := make([]*chanPartSegments, 0, len(mDimEntry))
	for _, entry := range mDimEntry {
		result = append(result, entry)
	}
	return result
}

// GetNumRowsOfCollection returns total rows count of segments belongs to provided collection
func (m *meta) GetNumRowsOfCollection(collectionID UniqueID) int64 {
	m.RLock()
	defer m.RUnlock()
	var ret int64
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.GetCollectionID() == collectionID {
			ret += segment.GetNumOfRows()
		}
	}
	return ret
}

// GetTotalBinlogSize returns the total size (bytes) of all segments in cluster.
func (m *meta) GetTotalBinlogSize() int64 {
	m.RLock()
	defer m.RUnlock()
	var ret int64
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		ret += segment.getSegmentSize()
	}
	return ret
}

// AddSegment records segment info, persisting info into kv store
func (m *meta) AddSegment(segment *SegmentInfo) error {
	log.Debug("meta update: adding segment",
		zap.Int64("segment ID", segment.GetID()))
	m.Lock()
	defer m.Unlock()
	if err := m.catalog.AddSegment(m.ctx, segment.SegmentInfo); err != nil {
		log.Error("meta update: adding segment failed",
			zap.Int64("segment ID", segment.GetID()),
			zap.Error(err))
		return err
	}
	m.segments.SetSegment(segment.GetID(), segment)
	metrics.DataCoordNumSegments.WithLabelValues(segment.GetState().String()).Inc()
	log.Debug("meta update: adding segment - complete",
		zap.Int64("segment ID", segment.GetID()))
	return nil
}

// DropSegment remove segment with provided id, etcd persistence also removed
func (m *meta) DropSegment(segmentID UniqueID) error {
	log.Debug("meta update: dropping segment",
		zap.Int64("segment ID", segmentID))
	m.Lock()
	defer m.Unlock()
	segment := m.segments.GetSegment(segmentID)
	if segment == nil {
		log.Warn("meta update: dropping segment failed - segment not found",
			zap.Int64("segment ID", segmentID))
		return nil
	}
	if err := m.catalog.DropSegment(m.ctx, segment.SegmentInfo); err != nil {
		log.Warn("meta update: dropping segment failed",
			zap.Int64("segment ID", segmentID),
			zap.Error(err))
		return err
	}
	metrics.DataCoordNumSegments.WithLabelValues(metrics.DropedSegmentLabel).Inc()
	m.segments.DropSegment(segmentID)
	log.Debug("meta update: dropping segment - complete",
		zap.Int64("segment ID", segmentID))
	return nil
}

// GetSegment returns segment info with provided id
// if not segment is found, nil will be returned
func (m *meta) GetSegment(segID UniqueID) *SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	segment := m.segments.GetSegment(segID)
	if segment != nil && isSegmentHealthy(segment) {
		return segment
	}
	return nil
}

// GetSegmentUnsafe returns segment info with provided id
// include the unhealthy segment
// if not segment is found, nil will be returned
func (m *meta) GetSegmentUnsafe(segID UniqueID) *SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	return m.segments.GetSegment(segID)
}

// GetAllSegmentsUnsafe returns all segments
func (m *meta) GetAllSegmentsUnsafe() []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	return m.segments.GetSegments()
}

// SetState setting segment with provided ID state
func (m *meta) SetState(segmentID UniqueID, targetState commonpb.SegmentState) error {
	log.Debug("meta update: setting segment state",
		zap.Int64("segment ID", segmentID),
		zap.Any("target state", targetState))
	m.Lock()
	defer m.Unlock()
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		log.Warn("meta update: setting segment state - segment not found",
			zap.Int64("segment ID", segmentID),
			zap.Any("target state", targetState))
		// TODO: Should probably return error instead.
		return nil
	}
	// Persist segment updates first.
	clonedSegment := curSegInfo.Clone()
	clonedSegment.State = targetState
	oldState := curSegInfo.GetState()
	if clonedSegment != nil && isSegmentHealthy(clonedSegment) {
		if err := m.catalog.AlterSegments(m.ctx, []*datapb.SegmentInfo{clonedSegment.SegmentInfo}); err != nil {
			log.Error("meta update: setting segment state - failed to alter segments",
				zap.Int64("segment ID", segmentID),
				zap.String("target state", targetState.String()),
				zap.Error(err))
			return err
		}
		metrics.DataCoordNumSegments.WithLabelValues(oldState.String()).Dec()
		metrics.DataCoordNumSegments.WithLabelValues(targetState.String()).Inc()
		if targetState == commonpb.SegmentState_Flushed {
			metrics.DataCoordNumStoredRows.WithLabelValues().Add(float64(curSegInfo.GetNumOfRows()))
			metrics.DataCoordNumStoredRowsCounter.WithLabelValues().Add(float64(curSegInfo.GetNumOfRows()))
		} else if oldState == commonpb.SegmentState_Flushed {
			metrics.DataCoordNumStoredRows.WithLabelValues().Sub(float64(curSegInfo.GetNumOfRows()))
		}
	}
	// Update in-memory meta.
	m.segments.SetState(segmentID, targetState)
	log.Debug("meta update: setting segment state - complete",
		zap.Int64("segment ID", segmentID),
		zap.String("target state", targetState.String()))
	return nil
}

// UnsetIsImporting removes the `isImporting` flag of a segment.
func (m *meta) UnsetIsImporting(segmentID UniqueID) error {
	log.Debug("meta update: unsetting isImport state of segment",
		zap.Int64("segment ID", segmentID))
	m.Lock()
	defer m.Unlock()
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		return fmt.Errorf("segment not found %d", segmentID)
	}
	// Persist segment updates first.
	clonedSegment := curSegInfo.Clone()
	clonedSegment.IsImporting = false
	if isSegmentHealthy(clonedSegment) {
		if err := m.catalog.AlterSegments(m.ctx, []*datapb.SegmentInfo{clonedSegment.SegmentInfo}); err != nil {
			log.Error("meta update: unsetting isImport state of segment - failed to unset segment isImporting state",
				zap.Int64("segment ID", segmentID),
				zap.Error(err))
			return err
		}
	}
	// Update in-memory meta.
	m.segments.SetIsImporting(segmentID, false)
	log.Debug("meta update: unsetting isImport state of segment - complete",
		zap.Int64("segment ID", segmentID))
	return nil
}

// UpdateFlushSegmentsInfo update segment partial/completed flush info
// `flushed` parameter indicating whether segment is flushed completely or partially
// `binlogs`, `checkpoints` and `statPositions` are persistence data for segment
func (m *meta) UpdateFlushSegmentsInfo(
	segmentID UniqueID,
	flushed bool,
	dropped bool,
	importing bool,
	binlogs, statslogs, deltalogs []*datapb.FieldBinlog,
	checkpoints []*datapb.CheckPoint,
	startPositions []*datapb.SegmentStartPosition,
) error {
	log.Debug("meta update: update flush segments info",
		zap.Int64("segmentId", segmentID),
		zap.Int("binlog", len(binlogs)),
		zap.Int("stats log", len(statslogs)),
		zap.Int("delta logs", len(deltalogs)),
		zap.Bool("flushed", flushed),
		zap.Bool("dropped", dropped),
		zap.Any("check points", checkpoints),
		zap.Any("start position", startPositions),
		zap.Bool("importing", importing))
	m.Lock()
	defer m.Unlock()

	segment := m.segments.GetSegment(segmentID)
	if importing {
		m.segments.SetRowCount(segmentID, segment.currRows)
		segment = m.segments.GetSegment(segmentID)
	}
	if segment == nil || !isSegmentHealthy(segment) {
		log.Warn("meta update: update flush segments info - segment not found",
			zap.Int64("segment ID", segmentID),
			zap.Bool("segment nil", segment == nil),
			zap.Bool("segment unhealthy", !isSegmentHealthy(segment)))
		return nil
	}

	clonedSegment := segment.Clone()
	modSegments := make(map[UniqueID]*SegmentInfo)
	if flushed {
		clonedSegment.State = commonpb.SegmentState_Flushing
		modSegments[segmentID] = clonedSegment
	}
	if dropped {
		clonedSegment.State = commonpb.SegmentState_Dropped
		clonedSegment.DroppedAt = uint64(time.Now().UnixNano())
		modSegments[segmentID] = clonedSegment
	}
	// TODO add diff encoding and compression
	currBinlogs := clonedSegment.GetBinlogs()
	var getFieldBinlogs = func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
		for _, binlog := range binlogs {
			if id == binlog.GetFieldID() {
				return binlog
			}
		}
		return nil
	}
	// binlogs
	for _, tBinlogs := range binlogs {
		fieldBinlogs := getFieldBinlogs(tBinlogs.GetFieldID(), currBinlogs)
		if fieldBinlogs == nil {
			currBinlogs = append(currBinlogs, tBinlogs)
		} else {
			fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, tBinlogs.Binlogs...)
		}
	}
	clonedSegment.Binlogs = currBinlogs
	// statlogs, overwrite latest segment stats log
	if len(statslogs) > 0 {
		clonedSegment.Statslogs = statslogs
	}
	// deltalogs
	currDeltaLogs := clonedSegment.GetDeltalogs()
	for _, tDeltaLogs := range deltalogs {
		fieldDeltaLogs := getFieldBinlogs(tDeltaLogs.GetFieldID(), currDeltaLogs)
		if fieldDeltaLogs == nil {
			currDeltaLogs = append(currDeltaLogs, tDeltaLogs)
		} else {
			fieldDeltaLogs.Binlogs = append(fieldDeltaLogs.Binlogs, tDeltaLogs.Binlogs...)
		}
	}
	clonedSegment.Deltalogs = currDeltaLogs
	modSegments[segmentID] = clonedSegment
	var getClonedSegment = func(segmentID UniqueID) *SegmentInfo {
		if s, ok := modSegments[segmentID]; ok {
			return s
		}
		if s := m.segments.GetSegment(segmentID); s != nil && isSegmentHealthy(s) {
			return s.Clone()
		}
		return nil
	}
	for _, pos := range startPositions {
		if len(pos.GetStartPosition().GetMsgID()) == 0 {
			continue
		}
		s := getClonedSegment(pos.GetSegmentID())
		if s == nil {
			continue
		}

		s.StartPosition = pos.GetStartPosition()
		modSegments[pos.GetSegmentID()] = s
	}
	for _, cp := range checkpoints {
		s := getClonedSegment(cp.GetSegmentID())
		if s == nil {
			continue
		}

		if s.DmlPosition != nil && s.DmlPosition.Timestamp >= cp.Position.Timestamp {
			// segment position in etcd is larger than checkpoint, then dont change it
			continue
		}

		s.DmlPosition = cp.GetPosition()
		s.NumOfRows = cp.GetNumOfRows()
		modSegments[cp.GetSegmentID()] = s
	}
	segments := make([]*datapb.SegmentInfo, 0, len(modSegments))
	for _, seg := range modSegments {
		segments = append(segments, seg.SegmentInfo)
	}
	if err := m.catalog.AlterSegments(m.ctx, segments); err != nil {
		log.Error("meta update: update flush segments info - failed to store flush segment info into Etcd",
			zap.Error(err))
		return err
	}
	oldSegmentState := segment.GetState()
	newSegmentState := clonedSegment.GetState()
	metrics.DataCoordNumSegments.WithLabelValues(oldSegmentState.String()).Dec()
	metrics.DataCoordNumSegments.WithLabelValues(newSegmentState.String()).Inc()
	if newSegmentState == commonpb.SegmentState_Flushed {
		metrics.DataCoordNumStoredRows.WithLabelValues().Add(float64(clonedSegment.GetNumOfRows()))
		metrics.DataCoordNumStoredRowsCounter.WithLabelValues().Add(float64(clonedSegment.GetNumOfRows()))
	} else if oldSegmentState == commonpb.SegmentState_Flushed {
		metrics.DataCoordNumStoredRows.WithLabelValues().Sub(float64(segment.GetNumOfRows()))
	}
	// update memory status
	for id, s := range modSegments {
		m.segments.SetSegment(id, s)
	}
	log.Debug("meta update: update flush segments info - update flush segments info successfully",
		zap.Int64("segment ID", segmentID))
	return nil
}

// UpdateDropChannelSegmentInfo updates segment checkpoints and binlogs before drop
// reusing segment info to pass segment id, binlogs, statslog, deltalog, start position and checkpoint
func (m *meta) UpdateDropChannelSegmentInfo(channel string, segments []*SegmentInfo) error {
	log.Debug("meta update: update drop channel segment info",
		zap.String("channel", channel))
	m.Lock()
	defer m.Unlock()
	modSegments := make(map[UniqueID]*SegmentInfo)
	originSegments := make(map[UniqueID]*SegmentInfo)
	// save new segments flushed from buffer data
	for _, seg2Drop := range segments {
		segment := m.mergeDropSegment(seg2Drop)
		if segment != nil {
			originSegments[seg2Drop.GetID()] = seg2Drop
			modSegments[seg2Drop.GetID()] = segment
		}
	}
	// set existed segments of channel to Dropped
	for _, seg := range m.segments.segments {
		if seg.InsertChannel != channel {
			continue
		}
		_, ok := modSegments[seg.ID]
		// seg inf mod segments are all in dropped state
		if !ok {
			clonedSeg := seg.Clone()
			clonedSeg.State = commonpb.SegmentState_Dropped
			modSegments[seg.ID] = clonedSeg
			originSegments[seg.GetID()] = seg
		}
	}
	err := m.batchSaveDropSegments(channel, modSegments)
	if err == nil {
		for _, seg := range originSegments {
			state := seg.GetState()
			metrics.DataCoordNumSegments.WithLabelValues(state.String()).Dec()
			if state == commonpb.SegmentState_Flushed {
				metrics.DataCoordNumStoredRows.WithLabelValues().Sub(float64(seg.GetNumOfRows()))
			}
		}
	}
	if err != nil {
		log.Error("meta update: update drop channel segment info failed",
			zap.String("channel", channel),
			zap.Error(err))
	} else {
		log.Debug("meta update: update drop channel segment info - complete",
			zap.String("channel", channel))
	}
	return err
}

// mergeDropSegment merges drop segment information with meta segments
func (m *meta) mergeDropSegment(seg2Drop *SegmentInfo) *SegmentInfo {
	segment := m.segments.GetSegment(seg2Drop.ID)
	// healthy check makes sure the Idempotence
	if segment == nil || !isSegmentHealthy(segment) {
		log.Warn("UpdateDropChannel skipping nil or unhealthy", zap.Bool("is nil", segment == nil),
			zap.Bool("isHealthy", isSegmentHealthy(segment)))
		return nil
	}

	clonedSegment := segment.Clone()
	clonedSegment.State = commonpb.SegmentState_Dropped

	currBinlogs := clonedSegment.GetBinlogs()

	var getFieldBinlogs = func(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
		for _, binlog := range binlogs {
			if id == binlog.GetFieldID() {
				return binlog
			}
		}
		return nil
	}
	// binlogs
	for _, tBinlogs := range seg2Drop.GetBinlogs() {
		fieldBinlogs := getFieldBinlogs(tBinlogs.GetFieldID(), currBinlogs)
		if fieldBinlogs == nil {
			currBinlogs = append(currBinlogs, tBinlogs)
		} else {
			fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, tBinlogs.Binlogs...)
		}
	}
	clonedSegment.Binlogs = currBinlogs
	// statlogs
	currStatsLogs := clonedSegment.GetStatslogs()
	for _, tStatsLogs := range seg2Drop.GetStatslogs() {
		fieldStatsLog := getFieldBinlogs(tStatsLogs.GetFieldID(), currStatsLogs)
		if fieldStatsLog == nil {
			currStatsLogs = append(currStatsLogs, tStatsLogs)
		} else {
			fieldStatsLog.Binlogs = append(fieldStatsLog.Binlogs, tStatsLogs.Binlogs...)
		}
	}
	clonedSegment.Statslogs = currStatsLogs
	// deltalogs
	clonedSegment.Deltalogs = append(clonedSegment.Deltalogs, seg2Drop.GetDeltalogs()...)

	// start position
	if seg2Drop.GetStartPosition() != nil {
		clonedSegment.StartPosition = seg2Drop.GetStartPosition()
	}
	// checkpoint
	if seg2Drop.GetDmlPosition() != nil {
		clonedSegment.DmlPosition = seg2Drop.GetDmlPosition()
	}
	clonedSegment.currRows = seg2Drop.currRows
	return clonedSegment
}

// batchSaveDropSegments saves drop segments info with channel removal flag
// since the channel unwatching operation is not atomic here
// ** the removal flag is always with last batch
// ** the last batch must contains at least one segment
// 1. when failure occurs between batches, failover mechanism will continue with the earlist  checkpoint of this channel
//   since the flag is not marked so DataNode can re-consume the drop collection msg
// 2. when failure occurs between save meta and unwatch channel, the removal flag shall be check before let datanode watch this channel
func (m *meta) batchSaveDropSegments(channel string, modSegments map[int64]*SegmentInfo) error {
	segments := make([]*datapb.SegmentInfo, 0)
	for _, seg := range modSegments {
		segments = append(segments, seg.SegmentInfo)
	}
	err := m.catalog.SaveDroppedSegmentsInBatch(m.ctx, segments)
	if err != nil {
		return err
	}

	if err = m.catalog.MarkChannelDeleted(m.ctx, channel); err != nil {
		return err
	}

	// update memory info
	for id, segment := range modSegments {
		m.segments.SetSegment(id, segment)
	}

	metrics.DataCoordNumSegments.WithLabelValues(metrics.DropedSegmentLabel).Add(float64(len(segments)))
	return nil
}

// GetSegmentsByChannel returns all segment info which insert channel equals provided `dmlCh`
func (m *meta) GetSegmentsByChannel(dmlCh string) []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	infos := make([]*SegmentInfo, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if !isSegmentHealthy(segment) || segment.InsertChannel != dmlCh {
			continue
		}
		infos = append(infos, segment)
	}
	return infos
}

// GetSegmentsOfCollection get all segments of collection
func (m *meta) GetSegmentsOfCollection(collectionID UniqueID) []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()

	ret := make([]*SegmentInfo, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.GetCollectionID() == collectionID {
			ret = append(ret, segment)
		}
	}
	return ret
}

// GetSegmentsIDOfCollection returns all segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollection(collectionID UniqueID) []UniqueID {
	m.RLock()
	defer m.RUnlock()
	ret := make([]UniqueID, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.CollectionID == collectionID {
			ret = append(ret, segment.ID)
		}
	}
	return ret
}

// GetSegmentsIDOfCollection returns all segment ids which collection equals to provided `collectionID`
func (m *meta) GetSegmentsIDOfCollectionWithDropped(collectionID UniqueID) []UniqueID {
	m.RLock()
	defer m.RUnlock()
	ret := make([]UniqueID, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if segment != nil &&
			segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
			segment.GetState() != commonpb.SegmentState_NotExist &&
			segment.CollectionID == collectionID {
			ret = append(ret, segment.ID)
		}
	}
	return ret
}

// GetSegmentsIDOfPartition returns all segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsIDOfPartition(collectionID, partitionID UniqueID) []UniqueID {
	m.RLock()
	defer m.RUnlock()
	ret := make([]UniqueID, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.CollectionID == collectionID && segment.PartitionID == partitionID {
			ret = append(ret, segment.ID)
		}
	}
	return ret
}

// GetSegmentsIDOfPartition returns all segments ids which collection & partition equals to provided `collectionID`, `partitionID`
func (m *meta) GetSegmentsIDOfPartitionWithDropped(collectionID, partitionID UniqueID) []UniqueID {
	m.RLock()
	defer m.RUnlock()
	ret := make([]UniqueID, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if segment != nil &&
			segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
			segment.GetState() != commonpb.SegmentState_NotExist &&
			segment.CollectionID == collectionID &&
			segment.PartitionID == partitionID {
			ret = append(ret, segment.ID)
		}
	}
	return ret
}

// GetNumRowsOfPartition returns row count of segments belongs to provided collection & partition
func (m *meta) GetNumRowsOfPartition(collectionID UniqueID, partitionID UniqueID) int64 {
	m.RLock()
	defer m.RUnlock()
	var ret int64
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if isSegmentHealthy(segment) && segment.CollectionID == collectionID && segment.PartitionID == partitionID {
			ret += segment.NumOfRows
		}
	}
	return ret
}

// GetUnFlushedSegments get all segments which state is not `Flushing` nor `Flushed`
func (m *meta) GetUnFlushedSegments() []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	ret := make([]*SegmentInfo, 0)
	segments := m.segments.GetSegments()
	for _, segment := range segments {
		if segment.State == commonpb.SegmentState_Growing || segment.State == commonpb.SegmentState_Sealed {
			ret = append(ret, segment)
		}
	}
	return ret
}

// GetFlushingSegments get all segments which state is `Flushing`
func (m *meta) GetFlushingSegments() []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	ret := make([]*SegmentInfo, 0)
	segments := m.segments.GetSegments()
	for _, info := range segments {
		if info.State == commonpb.SegmentState_Flushing {
			ret = append(ret, info)
		}
	}
	return ret
}

// SelectSegments select segments with selector
func (m *meta) SelectSegments(selector SegmentInfoSelector) []*SegmentInfo {
	m.RLock()
	defer m.RUnlock()
	var ret []*SegmentInfo
	segments := m.segments.GetSegments()
	for _, info := range segments {
		if selector(info) {
			ret = append(ret, info)
		}
	}
	return ret
}

// AddAllocation add allocation in segment
func (m *meta) AddAllocation(segmentID UniqueID, allocation *Allocation) error {
	log.Debug("meta update: add allocation",
		zap.Int64("segmentID", segmentID),
		zap.Any("allocation", allocation))
	m.Lock()
	defer m.Unlock()
	curSegInfo := m.segments.GetSegment(segmentID)
	if curSegInfo == nil {
		// TODO: Error handling.
		log.Warn("meta update: add allocation failed - segment not found",
			zap.Int64("segmentID", segmentID))
		return nil
	}
	// Persist segment updates first.
	clonedSegment := curSegInfo.Clone(AddAllocation(allocation))
	if clonedSegment != nil && isSegmentHealthy(clonedSegment) {
		if err := m.catalog.AlterSegments(m.ctx, []*datapb.SegmentInfo{clonedSegment.SegmentInfo}); err != nil {
			log.Error("meta update: add allocation failed",
				zap.Int64("segment ID", segmentID),
				zap.Error(err))
			return err
		}
	}
	// Update in-memory meta.
	m.segments.AddAllocation(segmentID, allocation)
	log.Debug("meta update: add allocation - complete",
		zap.Int64("segmentID", segmentID))
	return nil
}

// SetAllocations set Segment allocations, will overwrite ALL original allocations
// Note that allocations is not persisted in KV store
func (m *meta) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
	m.Lock()
	defer m.Unlock()
	m.segments.SetAllocations(segmentID, allocations)
}

// SetCurrentRows set current row count for segment with provided `segmentID`
// Note that currRows is not persisted in KV store
func (m *meta) SetCurrentRows(segmentID UniqueID, rows int64) {
	m.Lock()
	defer m.Unlock()
	m.segments.SetCurrentRows(segmentID, rows)
}

// SetLastFlushTime set LastFlushTime for segment with provided `segmentID`
// Note that lastFlushTime is not persisted in KV store
func (m *meta) SetLastFlushTime(segmentID UniqueID, t time.Time) {
	m.Lock()
	defer m.Unlock()
	m.segments.SetFlushTime(segmentID, t)
}

// SetSegmentCompacting sets compaction state for segment
func (m *meta) SetSegmentCompacting(segmentID UniqueID, compacting bool) {
	m.Lock()
	defer m.Unlock()

	m.segments.SetIsCompacting(segmentID, compacting)
}

// GetCompleteCompactionMeta returns
// - the segment info of compactedFrom segments before compaction to revert
// - the segment info of compactedFrom segments after compaction to alter
// - the segment info of compactedTo segment after compaction to add
// The compactedTo segment could contain 0 numRows
func (m *meta) GetCompleteCompactionMeta(compactionLogs []*datapb.CompactionSegmentBinlogs, result *datapb.CompactionResult) ([]*datapb.SegmentInfo, []*SegmentInfo, *SegmentInfo) {
	log.Info("meta update: get complete compaction meta")
	m.Lock()
	defer m.Unlock()

	var (
		oldSegments = make([]*datapb.SegmentInfo, 0, len(compactionLogs))
		modSegments = make([]*SegmentInfo, 0, len(compactionLogs))
	)

	for _, cl := range compactionLogs {
		if segment := m.segments.GetSegment(cl.GetSegmentID()); segment != nil {
			oldSegments = append(oldSegments, segment.Clone().SegmentInfo)

			cloned := segment.Clone()
			cloned.State = commonpb.SegmentState_Dropped
			cloned.DroppedAt = uint64(time.Now().UnixNano())
			modSegments = append(modSegments, cloned)
		}
	}

	var startPosition, dmlPosition *internalpb.MsgPosition
	for _, s := range modSegments {
		if dmlPosition == nil ||
			s.GetDmlPosition() != nil && s.GetDmlPosition().GetTimestamp() < dmlPosition.GetTimestamp() {
			dmlPosition = s.GetDmlPosition()
		}

		if startPosition == nil ||
			s.GetStartPosition() != nil && s.GetStartPosition().GetTimestamp() < startPosition.GetTimestamp() {
			startPosition = s.GetStartPosition()
		}
	}

	// find new added delta logs when executing compaction
	var originDeltalogs []*datapb.FieldBinlog
	for _, s := range modSegments {
		originDeltalogs = append(originDeltalogs, s.GetDeltalogs()...)
	}

	var deletedDeltalogs []*datapb.FieldBinlog
	for _, l := range compactionLogs {
		deletedDeltalogs = append(deletedDeltalogs, l.GetDeltalogs()...)
	}

	newAddedDeltalogs := m.updateDeltalogs(originDeltalogs, deletedDeltalogs, nil)
	deltalogs := append(result.GetDeltalogs(), newAddedDeltalogs...)

	compactionFrom := make([]UniqueID, 0, len(modSegments))
	for _, s := range modSegments {
		compactionFrom = append(compactionFrom, s.GetID())
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:                  result.GetSegmentID(),
		CollectionID:        modSegments[0].CollectionID,
		PartitionID:         modSegments[0].PartitionID,
		InsertChannel:       modSegments[0].InsertChannel,
		NumOfRows:           result.NumOfRows,
		State:               commonpb.SegmentState_Flushing,
		MaxRowNum:           modSegments[0].MaxRowNum,
		Binlogs:             result.GetInsertLogs(),
		Statslogs:           result.GetField2StatslogPaths(),
		Deltalogs:           deltalogs,
		StartPosition:       startPosition,
		DmlPosition:         dmlPosition,
		CreatedByCompaction: true,
		CompactionFrom:      compactionFrom,
	}
	segment := NewSegmentInfo(segmentInfo)

	log.Debug("meta update: get complete compaction meta - complete",
		zap.Int64("segmentID", segmentInfo.ID),
		zap.Int64("collectionID", segmentInfo.CollectionID),
		zap.Int64("partitionID", segmentInfo.PartitionID),
		zap.Int64("NumOfRows", segmentInfo.NumOfRows),
		zap.Any("compactionFrom", segmentInfo.CompactionFrom))

	return oldSegments, modSegments, segment
}

func (m *meta) alterMetaStoreAfterCompaction(modSegments []*datapb.SegmentInfo, newSegment *datapb.SegmentInfo) error {
	return m.catalog.AlterSegmentsAndAddNewSegment(m.ctx, modSegments, newSegment)
}

func (m *meta) revertAlterMetaStoreAfterCompaction(oldSegments []*datapb.SegmentInfo, removalSegment *datapb.SegmentInfo) error {
	log.Debug("revert metastore after compaction failure",
		zap.Int64("collectionID", removalSegment.CollectionID),
		zap.Int64("partitionID", removalSegment.PartitionID),
		zap.Int64("compactedTo", removalSegment.ID),
		zap.Int64s("compactedFrom", removalSegment.GetCompactionFrom()),
	)
	return m.catalog.RevertAlterSegmentsAndAddNewSegment(m.ctx, oldSegments, removalSegment)
}

func (m *meta) alterInMemoryMetaAfterCompaction(segmentCompactTo *SegmentInfo, segmentsCompactFrom []*SegmentInfo) {
	var compactFromIDs []int64
	for _, v := range segmentsCompactFrom {
		compactFromIDs = append(compactFromIDs, v.GetID())
	}
	log.Debug("meta update: alter in memory meta after compaction",
		zap.Int64("compact to segment ID", segmentCompactTo.GetID()),
		zap.Int64s("compact from segment IDs", compactFromIDs))
	m.Lock()
	defer m.Unlock()

	for _, s := range segmentsCompactFrom {
		m.segments.SetSegment(s.GetID(), s)
	}

	// Handle empty segment generated by merge-compaction
	if segmentCompactTo.GetNumOfRows() > 0 {
		m.segments.SetSegment(segmentCompactTo.GetID(), segmentCompactTo)
	}
	log.Debug("meta update: alter in memory meta after compaction - complete",
		zap.Int64("compact to segment ID", segmentCompactTo.GetID()),
		zap.Int64s("compact from segment IDs", compactFromIDs))
}

func (m *meta) updateBinlogs(origin []*datapb.FieldBinlog, removes []*datapb.FieldBinlog, adds []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	fieldBinlogs := make(map[int64]map[string]*datapb.Binlog)
	for _, f := range origin {
		fid := f.GetFieldID()
		if _, ok := fieldBinlogs[fid]; !ok {
			fieldBinlogs[fid] = make(map[string]*datapb.Binlog)
		}
		for _, p := range f.GetBinlogs() {
			fieldBinlogs[fid][p.GetLogPath()] = p
		}
	}

	for _, f := range removes {
		fid := f.GetFieldID()
		if _, ok := fieldBinlogs[fid]; !ok {
			continue
		}
		for _, p := range f.GetBinlogs() {
			delete(fieldBinlogs[fid], p.GetLogPath())
		}
	}

	for _, f := range adds {
		fid := f.GetFieldID()
		if _, ok := fieldBinlogs[fid]; !ok {
			fieldBinlogs[fid] = make(map[string]*datapb.Binlog)
		}
		for _, p := range f.GetBinlogs() {
			fieldBinlogs[fid][p.GetLogPath()] = p
		}
	}

	res := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
	for fid, logs := range fieldBinlogs {
		if len(logs) == 0 {
			continue
		}

		binlogs := make([]*datapb.Binlog, 0, len(logs))
		for _, log := range logs {
			binlogs = append(binlogs, log)
		}

		field := &datapb.FieldBinlog{FieldID: fid, Binlogs: binlogs}
		res = append(res, field)
	}
	return res
}

func (m *meta) updateDeltalogs(origin []*datapb.FieldBinlog, removes []*datapb.FieldBinlog, adds []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	res := make([]*datapb.FieldBinlog, 0, len(origin))
	for _, fbl := range origin {
		logs := make(map[string]*datapb.Binlog)
		for _, d := range fbl.GetBinlogs() {
			logs[d.GetLogPath()] = d
		}
		for _, remove := range removes {
			if remove.GetFieldID() == fbl.GetFieldID() {
				for _, r := range remove.GetBinlogs() {
					delete(logs, r.GetLogPath())
				}
			}
		}
		binlogs := make([]*datapb.Binlog, 0, len(logs))
		for _, l := range logs {
			binlogs = append(binlogs, l)
		}
		if len(binlogs) > 0 {
			res = append(res, &datapb.FieldBinlog{
				FieldID: fbl.GetFieldID(),
				Binlogs: binlogs,
			})
		}
	}

	return res
}

// buildSegment utility function for compose datapb.SegmentInfo struct with provided info
func buildSegment(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID, channelName string, isImporting bool) *SegmentInfo {
	info := &datapb.SegmentInfo{
		ID:            segmentID,
		CollectionID:  collectionID,
		PartitionID:   partitionID,
		InsertChannel: channelName,
		NumOfRows:     0,
		State:         commonpb.SegmentState_Growing,
		IsImporting:   isImporting,
	}
	return NewSegmentInfo(info)
}

func isSegmentHealthy(segment *SegmentInfo) bool {
	return segment != nil &&
		segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
		segment.GetState() != commonpb.SegmentState_NotExist &&
		segment.GetState() != commonpb.SegmentState_Dropped
}

func (m *meta) HasSegments(segIDs []UniqueID) (bool, error) {
	m.RLock()
	defer m.RUnlock()

	for _, segID := range segIDs {
		if _, ok := m.segments.segments[segID]; !ok {
			return false, fmt.Errorf("segment is not exist with ID = %d", segID)
		}
	}
	return true, nil
}
