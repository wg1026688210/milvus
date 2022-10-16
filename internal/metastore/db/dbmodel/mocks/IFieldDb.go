// Code generated by mockery v2.14.0. DO NOT EDIT.

package mocks

import (
	dbmodel "github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	mock "github.com/stretchr/testify/mock"
)

// IFieldDb is an autogenerated mock type for the IFieldDb type
type IFieldDb struct {
	mock.Mock
}

// GetByCollectionID provides a mock function with given fields: tenantID, collectionID, ts
func (_m *IFieldDb) GetByCollectionID(tenantID string, collectionID int64, ts uint64) ([]*dbmodel.Field, error) {
	ret := _m.Called(tenantID, collectionID, ts)

	var r0 []*dbmodel.Field
	if rf, ok := ret.Get(0).(func(string, int64, uint64) []*dbmodel.Field); ok {
		r0 = rf(tenantID, collectionID, ts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*dbmodel.Field)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, int64, uint64) error); ok {
		r1 = rf(tenantID, collectionID, ts)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Insert provides a mock function with given fields: in
func (_m *IFieldDb) Insert(in []*dbmodel.Field) error {
	ret := _m.Called(in)

	var r0 error
	if rf, ok := ret.Get(0).(func([]*dbmodel.Field) error); ok {
		r0 = rf(in)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockConstructorTestingTNewIFieldDb interface {
	mock.TestingT
	Cleanup(func())
}

// NewIFieldDb creates a new instance of IFieldDb. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewIFieldDb(t mockConstructorTestingTNewIFieldDb) *IFieldDb {
	mock := &IFieldDb{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
