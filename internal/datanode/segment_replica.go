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

package datanode

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/bits-and-blooms/bloom/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	maxBloomFalsePositive float64 = 0.005
)

type (
	primaryKey        = storage.PrimaryKey
	int64PrimaryKey   = storage.Int64PrimaryKey
	varCharPrimaryKey = storage.VarCharPrimaryKey
)

var (
	newInt64PrimaryKey   = storage.NewInt64PrimaryKey
	newVarCharPrimaryKey = storage.NewVarCharPrimaryKey
)

// Replica is DataNode unique replication
type Replica interface {
	getCollectionID() UniqueID
	getCollectionSchema(collectionID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error)
	getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error)
	getChannelName(segID UniqueID) (string, error)

	listAllSegmentIDs() []UniqueID
	listNotFlushedSegmentIDs() []UniqueID
	addSegment(req addSegmentReq) error
	listPartitionSegments(partID UniqueID) []UniqueID
	filterSegments(channelName string, partitionID UniqueID) []*Segment
	listNewSegmentsStartPositions() []*datapb.SegmentStartPosition
	listSegmentsCheckPoints() map[UniqueID]segmentCheckPoint
	updateSegmentEndPosition(segID UniqueID, endPos *internalpb.MsgPosition)
	updateSegmentCheckPoint(segID UniqueID)
	updateSegmentPKRange(segID UniqueID, ids storage.FieldData)
	mergeFlushedSegments(seg *Segment, planID UniqueID, compactedFrom []UniqueID) error
	hasSegment(segID UniqueID, countFlushed bool) bool
	removeSegments(segID ...UniqueID)
	listCompactedSegmentIDs() map[UniqueID][]UniqueID

	updateStatistics(segID UniqueID, numRows int64)
	refreshFlushedSegStatistics(segID UniqueID, numRows int64)
	getSegmentStatisticsUpdates(segID UniqueID) (*datapb.SegmentStats, error)
	segmentFlushed(segID UniqueID)
	getSegmentStatslog(segID UniqueID) ([]byte, error)
	initSegmentBloomFilter(seg *Segment) error
}

// Segment is the data structure of segments in data node replica.
type Segment struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	numRows      int64
	memorySize   int64
	isNew        atomic.Value // bool
	isFlushed    atomic.Value // bool
	channelName  string
	compactedTo  UniqueID

	checkPoint segmentCheckPoint
	startPos   *internalpb.MsgPosition // TODO readonly
	endPos     *internalpb.MsgPosition

	pkFilter *bloom.BloomFilter //  bloom filter of pk inside a segment
	minPK    primaryKey         //	minimal pk value, shortcut for checking whether a pk is inside this segment
	maxPK    primaryKey         //  maximal pk value, same above
}

// SegmentReplica is the data replication of persistent data in datanode.
// It implements `Replica` interface.
type SegmentReplica struct {
	collectionID UniqueID
	collSchema   *schemapb.CollectionSchema
	schemaMut    sync.RWMutex

	segMu             sync.RWMutex
	newSegments       map[UniqueID]*Segment
	normalSegments    map[UniqueID]*Segment
	flushedSegments   map[UniqueID]*Segment
	compactedSegments map[UniqueID]*Segment

	metaService  *metaService
	chunkManager storage.ChunkManager
}

type addSegmentReq struct {
	segType                    datapb.SegmentType
	segID, collID, partitionID UniqueID
	channelName                string
	numOfRows                  int64
	startPos, endPos           *internalpb.MsgPosition
	statsBinLogs               []*datapb.FieldBinlog
	cp                         *segmentCheckPoint
	recoverTs                  Timestamp
	importing                  bool
}

func (s *Segment) updatePk(pk primaryKey) error {
	if s.minPK == nil {
		s.minPK = pk
	} else if s.minPK.GT(pk) {
		s.minPK = pk
	}

	if s.maxPK == nil {
		s.maxPK = pk
	} else if s.maxPK.LT(pk) {
		s.maxPK = pk
	}

	return nil
}

func (s *Segment) updatePKRange(ids storage.FieldData) error {
	switch pks := ids.(type) {
	case *storage.Int64FieldData:
		buf := make([]byte, 8)
		for _, pk := range pks.Data {
			id := newInt64PrimaryKey(pk)
			err := s.updatePk(id)
			if err != nil {
				return err
			}
			common.Endian.PutUint64(buf, uint64(pk))
			s.pkFilter.Add(buf)
		}
	case *storage.StringFieldData:
		for _, pk := range pks.Data {
			id := newVarCharPrimaryKey(pk)
			err := s.updatePk(id)
			if err != nil {
				return err
			}
			s.pkFilter.AddString(pk)
		}
	default:
		//TODO::
	}

	log.Info("update pk range",
		zap.Int64("collectionID", s.collectionID), zap.Int64("partitionID", s.partitionID), zap.Int64("segmentID", s.segmentID),
		zap.String("channel", s.channelName),
		zap.Int64("num_rows", s.numRows), zap.Any("minPK", s.minPK), zap.Any("maxPK", s.maxPK))

	return nil
}

func (s *Segment) getSegmentStatslog(pkID UniqueID, pkType schemapb.DataType) ([]byte, error) {
	pks := storage.PrimaryKeyStats{
		FieldID: pkID,
		PkType:  int64(pkType),
		MaxPk:   s.maxPK,
		MinPk:   s.minPK,
		BF:      s.pkFilter,
	}

	return json.Marshal(pks)
}

var _ Replica = &SegmentReplica{}

func newReplica(ctx context.Context, rc types.RootCoord, cm storage.ChunkManager, collID UniqueID, schema *schemapb.CollectionSchema) (*SegmentReplica, error) {
	metaService := newMetaService(rc, collID)

	replica := &SegmentReplica{
		collectionID: collID,
		collSchema:   schema,

		newSegments:       make(map[UniqueID]*Segment),
		normalSegments:    make(map[UniqueID]*Segment),
		flushedSegments:   make(map[UniqueID]*Segment),
		compactedSegments: make(map[UniqueID]*Segment),

		metaService:  metaService,
		chunkManager: cm,
	}

	return replica, nil
}

// segmentFlushed transfers a segment from *New* or *Normal* into *Flushed*.
func (replica *SegmentReplica) segmentFlushed(segID UniqueID) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	if _, ok := replica.newSegments[segID]; ok {
		replica.new2FlushedSegment(segID)
	}

	if _, ok := replica.normalSegments[segID]; ok {
		replica.normal2FlushedSegment(segID)
	}
}

func (replica *SegmentReplica) new2NormalSegment(segID UniqueID) {
	var seg = *replica.newSegments[segID]

	seg.isNew.Store(false)
	replica.normalSegments[segID] = &seg

	delete(replica.newSegments, segID)
}

func (replica *SegmentReplica) new2FlushedSegment(segID UniqueID) {
	var seg = *replica.newSegments[segID]

	seg.isNew.Store(false)
	seg.isFlushed.Store(true)
	replica.flushedSegments[segID] = &seg

	delete(replica.newSegments, segID)
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Dec()
}

// normal2FlushedSegment transfers a segment from *normal* to *flushed* by changing *isFlushed*
//  flag into true, and mv the segment from normalSegments map to flushedSegments map.
func (replica *SegmentReplica) normal2FlushedSegment(segID UniqueID) {
	var seg = *replica.normalSegments[segID]

	seg.isFlushed.Store(true)
	replica.flushedSegments[segID] = &seg

	delete(replica.normalSegments, segID)
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Dec()
}

func (replica *SegmentReplica) getCollectionAndPartitionID(segID UniqueID) (collID, partitionID UniqueID, err error) {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	if seg, ok := replica.newSegments[segID]; ok {
		return seg.collectionID, seg.partitionID, nil
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		return seg.collectionID, seg.partitionID, nil
	}

	if seg, ok := replica.flushedSegments[segID]; ok {
		return seg.collectionID, seg.partitionID, nil
	}

	return 0, 0, fmt.Errorf("cannot find segment, id = %v", segID)
}

func (replica *SegmentReplica) getChannelName(segID UniqueID) (string, error) {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	if seg, ok := replica.newSegments[segID]; ok {
		return seg.channelName, nil
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		return seg.channelName, nil
	}

	if seg, ok := replica.flushedSegments[segID]; ok {
		return seg.channelName, nil
	}

	return "", fmt.Errorf("cannot find segment, id = %v", segID)
}

// maxRowCountPerSegment returns max row count for a segment based on estimation of row size.
func (replica *SegmentReplica) maxRowCountPerSegment(ts Timestamp) (int64, error) {
	log := log.With(zap.Int64("collectionID", replica.collectionID), zap.Uint64("timpstamp", ts))
	schema, err := replica.getCollectionSchema(replica.collectionID, ts)
	if err != nil {
		log.Warn("failed to get collection schema", zap.Error(err))
		return 0, err
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		log.Warn("failed to estimate size per record", zap.Error(err))
		return 0, err
	}
	threshold := Params.DataCoordCfg.SegmentMaxSize * 1024 * 1024
	return int64(threshold / float64(sizePerRecord)), nil
}

// initSegmentBloomFilter initialize segment pkFilter with a new bloom filter.
// this new BF will be initialized with estimated max rows and default false positive rate.
func (replica *SegmentReplica) initSegmentBloomFilter(s *Segment) error {
	var ts Timestamp
	if s.startPos != nil {
		ts = s.startPos.Timestamp
	}
	maxRowCount, err := replica.maxRowCountPerSegment(ts)
	if err != nil {
		log.Warn("initSegmentBloomFilter failed, cannot estimate max row count", zap.Error(err))
		return err
	}

	s.pkFilter = bloom.NewWithEstimates(uint(maxRowCount), maxBloomFalsePositive)
	return nil
}

// addSegment adds the segment to current replica. Segments can be added as *new*, *normal* or *flushed*.
// Make sure to verify `replica.hasSegment(segID)` == false before calling `replica.addSegment()`.
func (replica *SegmentReplica) addSegment(req addSegmentReq) error {
	if req.collID != replica.collectionID {
		log.Warn("collection mismatch",
			zap.Int64("current collection ID", req.collID),
			zap.Int64("expected collection ID", replica.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", req.collID)
	}
	log.Info("adding segment",
		zap.String("segment type", req.segType.String()),
		zap.Int64("segment ID", req.segID),
		zap.Int64("collection ID", req.collID),
		zap.Int64("partition ID", req.partitionID),
		zap.String("channel name", req.channelName),
		zap.Any("start position", req.startPos),
		zap.Any("end position", req.endPos),
		zap.Any("checkpoints", req.cp),
		zap.Uint64("recover ts", req.recoverTs),
		zap.Bool("importing", req.importing),
	)
	seg := &Segment{
		collectionID: req.collID,
		partitionID:  req.partitionID,
		segmentID:    req.segID,
		channelName:  req.channelName,
		numRows:      req.numOfRows, // 0 if segType == NEW
	}
	if req.importing || req.segType == datapb.SegmentType_New {
		seg.checkPoint = segmentCheckPoint{0, *req.startPos}
		seg.startPos = req.startPos
		seg.endPos = req.endPos
	}
	if req.segType == datapb.SegmentType_Normal {
		if req.cp != nil {
			seg.checkPoint = *req.cp
			seg.endPos = &req.cp.pos
		}
	}
	// Set up bloom filter.
	err := replica.initPKBloomFilter(context.TODO(), seg, req.statsBinLogs, req.recoverTs)
	if err != nil {
		log.Error("failed to init bloom filter",
			zap.Int64("segment ID", req.segID),
			zap.Error(err))
		return err
	}
	// Please ignore `isNew` and `isFlushed` as they are for debugging only.
	if req.segType == datapb.SegmentType_New {
		seg.isNew.Store(true)
	} else {
		seg.isNew.Store(false)
	}
	if req.segType == datapb.SegmentType_Flushed {
		seg.isFlushed.Store(true)
	} else {
		seg.isFlushed.Store(false)
	}
	replica.segMu.Lock()
	if req.segType == datapb.SegmentType_New {
		replica.newSegments[req.segID] = seg
	} else if req.segType == datapb.SegmentType_Normal {
		replica.normalSegments[req.segID] = seg
	} else if req.segType == datapb.SegmentType_Flushed {
		replica.flushedSegments[req.segID] = seg
	}
	replica.segMu.Unlock()
	if req.segType == datapb.SegmentType_New || req.segType == datapb.SegmentType_Normal {
		metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Inc()
	}
	return nil
}

func (replica *SegmentReplica) listCompactedSegmentIDs() map[UniqueID][]UniqueID {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	compactedTo2From := make(map[UniqueID][]UniqueID)

	for segID, seg := range replica.compactedSegments {
		compactedTo2From[seg.compactedTo] = append(compactedTo2From[seg.compactedTo], segID)
	}

	return compactedTo2From
}

// filterSegments return segments with same channelName and partition ID
// get all segments
func (replica *SegmentReplica) filterSegments(channelName string, partitionID UniqueID) []*Segment {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()
	results := make([]*Segment, 0)

	isMatched := func(segment *Segment, chanName string, partID UniqueID) bool {
		return segment.channelName == chanName && (partID == common.InvalidPartitionID || segment.partitionID == partID)
	}
	for _, seg := range replica.newSegments {
		if isMatched(seg, channelName, partitionID) {
			results = append(results, seg)
		}
	}
	for _, seg := range replica.normalSegments {
		if isMatched(seg, channelName, partitionID) {
			results = append(results, seg)
		}
	}
	for _, seg := range replica.flushedSegments {
		if isMatched(seg, channelName, partitionID) {
			results = append(results, seg)
		}
	}
	return results
}

func (replica *SegmentReplica) initPKBloomFilter(ctx context.Context, s *Segment, statsBinlogs []*datapb.FieldBinlog, ts Timestamp) error {
	log := log.With(zap.Int64("segmentID", s.segmentID))
	log.Info("begin to init pk bloom filter", zap.Int("stats bin logs", len(statsBinlogs)))
	schema, err := replica.getCollectionSchema(s.collectionID, ts)
	if err != nil {
		log.Warn("failed to initPKBloomFilter, get schema return error", zap.Error(err))
		return err
	}

	// get pkfield id
	pkField := int64(-1)
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			pkField = field.FieldID
			break
		}
	}

	// filter stats binlog files which is pk field stats log
	var bloomFilterFiles []string
	for _, binlog := range statsBinlogs {
		if binlog.FieldID != pkField {
			continue
		}
		for _, log := range binlog.GetBinlogs() {
			bloomFilterFiles = append(bloomFilterFiles, log.GetLogPath())
		}
	}

	// no stats log to parse, initialize a new BF
	if len(bloomFilterFiles) == 0 {
		log.Warn("no stats files to load, initializa a new one")
		return replica.initSegmentBloomFilter(s)
	}

	values, err := replica.chunkManager.MultiRead(ctx, bloomFilterFiles)
	if err != nil {
		log.Warn("failed to load bloom filter files", zap.Error(err))
		return err
	}
	blobs := make([]*Blob, 0)
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &Blob{Value: values[i]})
	}

	stats, err := storage.DeserializeStats(blobs)
	if err != nil {
		log.Warn("failed to deserialize bloom filter files", zap.Error(err))
		return err
	}
	for _, stat := range stats {
		// use first BF to merge
		if s.pkFilter == nil {
			s.pkFilter = stat.BF
		} else {
			// for compatibility, statslog before 2.1.2 uses separated stats log which needs to be merged
			// assuming all legacy BF has same attributes.
			err = s.pkFilter.Merge(stat.BF)
			if err != nil {
				return err
			}
		}

		s.updatePk(stat.MinPk)
		s.updatePk(stat.MaxPk)
	}

	return nil
}

// listNewSegmentsStartPositions gets all *New Segments* start positions and
//   transfer segments states from *New* to *Normal*.
func (replica *SegmentReplica) listNewSegmentsStartPositions() []*datapb.SegmentStartPosition {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	result := make([]*datapb.SegmentStartPosition, 0, len(replica.newSegments))
	for id, seg := range replica.newSegments {

		result = append(result, &datapb.SegmentStartPosition{
			SegmentID:     id,
			StartPosition: seg.startPos,
		})

		// transfer states
		replica.new2NormalSegment(id)
	}
	return result
}

// listSegmentsCheckPoints gets check points from both *New* and *Normal* segments.
func (replica *SegmentReplica) listSegmentsCheckPoints() map[UniqueID]segmentCheckPoint {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	result := make(map[UniqueID]segmentCheckPoint)

	for id, seg := range replica.newSegments {
		result[id] = seg.checkPoint
	}

	for id, seg := range replica.normalSegments {
		result[id] = seg.checkPoint
	}

	return result
}

// updateSegmentEndPosition updates *New* or *Normal* segment's end position.
func (replica *SegmentReplica) updateSegmentEndPosition(segID UniqueID, endPos *internalpb.MsgPosition) {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	seg, ok := replica.newSegments[segID]
	if ok {
		seg.endPos = endPos
		return
	}

	seg, ok = replica.normalSegments[segID]
	if ok {
		seg.endPos = endPos
		return
	}

	log.Warn("No match segment", zap.Int64("ID", segID))
}

func (replica *SegmentReplica) updateSegmentPKRange(segID UniqueID, ids storage.FieldData) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	seg, ok := replica.newSegments[segID]
	if ok {
		seg.updatePKRange(ids)
		return
	}

	seg, ok = replica.normalSegments[segID]
	if ok {
		seg.updatePKRange(ids)
		return
	}

	seg, ok = replica.flushedSegments[segID]
	if ok {
		seg.updatePKRange(ids)
		return
	}

	log.Warn("No match segment to update PK range", zap.Int64("ID", segID))
}

func (replica *SegmentReplica) removeSegments(segIDs ...UniqueID) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	log.Info("remove segments if exist", zap.Int64s("segmentIDs", segIDs))
	cnt := 0
	for _, segID := range segIDs {
		if _, ok := replica.newSegments[segID]; ok {
			cnt++
		} else if _, ok := replica.normalSegments[segID]; ok {
			cnt++
		}
	}
	metrics.DataNodeNumUnflushedSegments.WithLabelValues(fmt.Sprint(Params.DataNodeCfg.GetNodeID())).Sub(float64(cnt))

	for _, segID := range segIDs {
		delete(replica.newSegments, segID)
		delete(replica.normalSegments, segID)
		delete(replica.flushedSegments, segID)
		delete(replica.compactedSegments, segID)
	}
}

// hasSegment checks whether this replica has a segment according to segment ID.
func (replica *SegmentReplica) hasSegment(segID UniqueID, countFlushed bool) bool {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	_, inNew := replica.newSegments[segID]
	_, inNormal := replica.normalSegments[segID]

	inFlush := false
	if countFlushed {
		_, inFlush = replica.flushedSegments[segID]
	}

	return inNew || inNormal || inFlush
}
func (replica *SegmentReplica) refreshFlushedSegStatistics(segID UniqueID, numRows int64) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	if seg, ok := replica.flushedSegments[segID]; ok {
		seg.memorySize = 0
		seg.numRows = numRows
		return
	}

	log.Warn("refresh numRow on not exists segment", zap.Int64("segID", segID))
}

// updateStatistics updates the number of rows of a segment in replica.
func (replica *SegmentReplica) updateStatistics(segID UniqueID, numRows int64) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	log.Info("updating segment", zap.Int64("Segment ID", segID), zap.Int64("numRows", numRows))
	if seg, ok := replica.newSegments[segID]; ok {
		seg.memorySize = 0
		seg.numRows += numRows
		return
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		seg.memorySize = 0
		seg.numRows += numRows
		return
	}

	log.Warn("update segment num row not exist", zap.Int64("segID", segID))
}

// getSegmentStatisticsUpdates gives current segment's statistics updates.
func (replica *SegmentReplica) getSegmentStatisticsUpdates(segID UniqueID) (*datapb.SegmentStats, error) {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()
	updates := &datapb.SegmentStats{SegmentID: segID}

	if seg, ok := replica.newSegments[segID]; ok {
		updates.NumRows = seg.numRows
		return updates, nil
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		updates.NumRows = seg.numRows
		return updates, nil
	}

	if seg, ok := replica.flushedSegments[segID]; ok {
		updates.NumRows = seg.numRows
		return updates, nil
	}

	return nil, fmt.Errorf("error, there's no segment %v", segID)
}

// --- collection ---
func (replica *SegmentReplica) getCollectionID() UniqueID {
	return replica.collectionID
}

// getCollectionSchema gets collection schema from rootcoord for a certain timestamp.
//   If you want the latest collection schema, ts should be 0.
func (replica *SegmentReplica) getCollectionSchema(collID UniqueID, ts Timestamp) (*schemapb.CollectionSchema, error) {
	if !replica.validCollection(collID) {
		return nil, fmt.Errorf("mismatch collection, want %d, actual %d", replica.collectionID, collID)
	}

	replica.schemaMut.RLock()
	if replica.collSchema == nil {
		replica.schemaMut.RUnlock()

		replica.schemaMut.Lock()
		defer replica.schemaMut.Unlock()
		if replica.collSchema == nil {
			sch, err := replica.metaService.getCollectionSchema(context.Background(), collID, ts)
			if err != nil {
				return nil, err
			}
			replica.collSchema = sch
		}
	} else {
		defer replica.schemaMut.RUnlock()
	}

	return replica.collSchema, nil
}

func (replica *SegmentReplica) validCollection(collID UniqueID) bool {
	return collID == replica.collectionID
}

// updateSegmentCheckPoint is called when auto flush or mannul flush is done.
func (replica *SegmentReplica) updateSegmentCheckPoint(segID UniqueID) {
	replica.segMu.Lock()
	defer replica.segMu.Unlock()

	if seg, ok := replica.newSegments[segID]; ok {
		seg.checkPoint = segmentCheckPoint{seg.numRows, *seg.endPos}
		return
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		seg.checkPoint = segmentCheckPoint{seg.numRows, *seg.endPos}
		return
	}

	log.Warn("There's no segment", zap.Int64("ID", segID))
}

func (replica *SegmentReplica) mergeFlushedSegments(seg *Segment, planID UniqueID, compactedFrom []UniqueID) error {

	log := log.With(
		zap.Int64("segment ID", seg.segmentID),
		zap.Int64("collection ID", seg.collectionID),
		zap.Int64("partition ID", seg.partitionID),
		zap.Int64s("compacted from", compactedFrom),
		zap.Int64("planID", planID),
		zap.String("channel name", seg.channelName))

	if seg.collectionID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("expected collectionID", replica.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", seg.collectionID)
	}

	var inValidSegments []UniqueID
	for _, ID := range compactedFrom {
		// no such segments in replica or the segments are unflushed.
		if !replica.hasSegment(ID, true) || replica.hasSegment(ID, false) {
			inValidSegments = append(inValidSegments, ID)
		}
	}

	if len(inValidSegments) > 0 {
		log.Warn("no match flushed segments to merge from", zap.Int64s("invalid segmentIDs", inValidSegments))
		return fmt.Errorf("invalid compactedFrom segments: %v", inValidSegments)
	}

	replica.segMu.Lock()
	log.Info("merge flushed segments")
	for _, ID := range compactedFrom {
		// the existent of the segments are already checked
		s := replica.flushedSegments[ID]

		s.compactedTo = seg.segmentID
		replica.compactedSegments[ID] = s
		delete(replica.flushedSegments, ID)
	}
	replica.segMu.Unlock()

	// only store segments with numRows > 0
	if seg.numRows > 0 {
		seg.isNew.Store(false)
		seg.isFlushed.Store(true)

		replica.segMu.Lock()
		replica.flushedSegments[seg.segmentID] = seg
		replica.segMu.Unlock()
	}

	return nil
}

// for tests only
func (replica *SegmentReplica) addFlushedSegmentWithPKs(segID, collID, partID UniqueID, channelName string, numOfRows int64, ids storage.FieldData) error {
	if collID != replica.collectionID {
		log.Warn("Mismatch collection",
			zap.Int64("input ID", collID),
			zap.Int64("expected ID", replica.collectionID))
		return fmt.Errorf("mismatch collection, ID=%d", collID)
	}

	log.Info("Add Flushed segment",
		zap.Int64("segment ID", segID),
		zap.Int64("collection ID", collID),
		zap.Int64("partition ID", partID),
		zap.String("channel name", channelName),
	)

	seg := &Segment{
		collectionID: collID,
		partitionID:  partID,
		segmentID:    segID,
		channelName:  channelName,
		numRows:      numOfRows,
	}

	err := replica.initSegmentBloomFilter(seg)
	if err != nil {
		return err
	}

	seg.updatePKRange(ids)

	seg.isNew.Store(false)
	seg.isFlushed.Store(true)

	replica.segMu.Lock()
	replica.flushedSegments[segID] = seg
	replica.segMu.Unlock()

	return nil
}

func (replica *SegmentReplica) listAllSegmentIDs() []UniqueID {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	var segIDs []UniqueID

	for _, seg := range replica.newSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	for _, seg := range replica.normalSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	for _, seg := range replica.flushedSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	return segIDs
}

func (replica *SegmentReplica) listPartitionSegments(partID UniqueID) []UniqueID {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	var segIDs []UniqueID

	for _, seg := range replica.newSegments {
		if seg.partitionID == partID {
			segIDs = append(segIDs, seg.segmentID)
		}
	}

	for _, seg := range replica.normalSegments {
		if seg.partitionID == partID {
			segIDs = append(segIDs, seg.segmentID)
		}
	}

	for _, seg := range replica.flushedSegments {
		if seg.partitionID == partID {
			segIDs = append(segIDs, seg.segmentID)
		}
	}

	return segIDs
}

func (replica *SegmentReplica) listNotFlushedSegmentIDs() []UniqueID {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()

	var segIDs []UniqueID

	for _, seg := range replica.newSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	for _, seg := range replica.normalSegments {
		segIDs = append(segIDs, seg.segmentID)
	}

	return segIDs
}

// getSegmentStatslog returns the segment statslog for the provided segment id.
func (replica *SegmentReplica) getSegmentStatslog(segID UniqueID) ([]byte, error) {
	replica.segMu.RLock()
	defer replica.segMu.RUnlock()
	colID := replica.getCollectionID()

	schema, err := replica.getCollectionSchema(colID, 0)
	if err != nil {
		return nil, err
	}

	var pkID UniqueID
	var pkType schemapb.DataType
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			pkID = field.GetFieldID()
			pkType = field.GetDataType()
		}
	}

	if seg, ok := replica.newSegments[segID]; ok {
		return seg.getSegmentStatslog(pkID, pkType)
	}

	if seg, ok := replica.normalSegments[segID]; ok {
		return seg.getSegmentStatslog(pkID, pkType)
	}

	if seg, ok := replica.flushedSegments[segID]; ok {
		return seg.getSegmentStatslog(pkID, pkType)
	}

	return nil, fmt.Errorf("segment not found: %d", segID)
}
