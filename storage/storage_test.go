package storage

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-amqp-common-go/v3/aad"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Azure/azure-event-hubs-go/v3/eph"
	"github.com/Azure/azure-event-hubs-go/v3/internal/test"
	"github.com/Azure/azure-event-hubs-go/v3/persist"
)

const (
	shortTimeout = 30 * time.Second
)

func (ts *testSuite) TestSharedKeyCredential() {
	cred, err := azblob.NewSharedKeyCredential("foo", "Zm9vCg==")
	ts.Require().NoError(err)
	leaser, err := NewStorageLeaserCheckpointer(cred, ts.AccountName, "someContainer", ts.Env)
	ts.NoError(err)
	ts.NotNil(leaser)
}

func (ts *testSuite) TestLeaserStoreCreation() {
	leaser, del := ts.newLeaser()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()
	exists, err := leaser.StoreExists(ctx)
	ts.Require().NoError(err)
	ts.False(exists)

	err = leaser.EnsureStore(ctx)
	ts.Require().NoError(err)

	exists, err = leaser.StoreExists(ctx)
	ts.NoError(err)
	ts.True(exists)
}

func TestLeaserConcurrentStoring(t *testing.T) {
	ch := make(chan struct{})
	wg := sync.WaitGroup{}

	env := test.GetTestEnv()

	containerName := strings.ToLower(test.RandomString(fmt.Sprintf("concurrentstortest-%s", env.TagID), 4))

	cred, err := NewAADSASCredential(env.SubscriptionID, env.ResourceGroupName, env.StorageAccountName, containerName, AADSASCredentialWithEnvironmentVars())
	require.NoError(t, err)
	leaser, err := NewStorageLeaserCheckpointer(cred, env.StorageAccountName, containerName, env.Env)
	defer leaser.DeleteStore(context.Background())

	tryToGetLease := func() {
		defer wg.Done()
		<-ch

		cred, err := NewAADSASCredential(env.SubscriptionID, env.ResourceGroupName, env.StorageAccountName, containerName, AADSASCredentialWithEnvironmentVars())
		require.NoError(t, err)
		leaser, err := NewStorageLeaserCheckpointer(cred, env.StorageAccountName, containerName, env.Env)
		require.NoError(t, err)

		_, err = leaser.StoreExists(context.Background())
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			_, _, err := leaser.AcquireLease(context.Background(), "0")
			require.NoError(t, err)
		}
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go tryToGetLease()
	}

	close(ch)
	wg.Wait()
}

func TestLeaserConcurrentCheckpointStoring(t *testing.T) {
	ch := make(chan struct{})
	wg := sync.WaitGroup{}

	env := test.GetTestEnv()

	containerName := strings.ToLower(test.RandomString(fmt.Sprintf("concurrentstortest-%s", env.TagID), 4))

	cred, err := NewAADSASCredential(env.SubscriptionID, env.ResourceGroupName, env.StorageAccountName, containerName, AADSASCredentialWithEnvironmentVars())
	require.NoError(t, err)
	leaser, err := NewStorageLeaserCheckpointer(cred, env.StorageAccountName, containerName, env.Env)
	defer leaser.DeleteStore(context.Background())

	tryToGetLease := func() {
		defer wg.Done()
		<-ch

		cred, err := NewAADSASCredential(env.SubscriptionID, env.ResourceGroupName, env.StorageAccountName, containerName, AADSASCredentialWithEnvironmentVars())
		require.NoError(t, err)
		leaser, err := NewStorageLeaserCheckpointer(cred, env.StorageAccountName, containerName, env.Env)
		require.NoError(t, err)

		for i := 0; i < 100; i++ {
			_, isOwner, err := leaser.RenewLease(context.Background(), "0")
			require.NoError(t, err)

			if isOwner {
				cp := persist.NewCheckpoint("0", 0, time.Now())
				require.NoError(t, leaser.UpdateCheckpoint(context.Background(), "0", cp))
			}
		}
	}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go tryToGetLease()
	}

	close(ch)
	wg.Wait()
}

func (ts *testSuite) TestLeaserLeaseEnsure() {
	leaser, del := ts.leaserWithEPH()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()
	for _, partitionID := range leaser.processor.GetPartitionIDs() {
		lease, err := leaser.EnsureLease(ctx, partitionID)
		ts.NoError(err)
		ts.Equal(partitionID, lease.GetPartitionID())
	}
}

func (ts *testSuite) TestLeaserAcquire() {
	leaser, del := ts.leaserWithEPHAndLeases()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()
	leases, err := leaser.GetLeases(ctx)
	ts.Require().NoError(err)
	assert.Equal(ts.T(), len(leaser.processor.GetPartitionIDs()), len(leases))

	for _, lease := range leases {
		epochBefore := lease.GetEpoch()
		acquiredLease, ok, err := leaser.AcquireLease(ctx, lease.GetPartitionID())
		ts.Require().NoError(err)
		ts.Require().True(ok, "should have acquired the lease")
		assert.Equal(ts.T(), epochBefore+1, acquiredLease.GetEpoch())
		assert.Equal(ts.T(), leaser.processor.GetName(), acquiredLease.GetOwner())
		assert.NotNil(ts.T(), acquiredLease.(*storageLease).Token)
	}
	assert.Equal(ts.T(), len(leaser.processor.GetPartitionIDs()), len(leaser.leases))
}

func (ts *testSuite) TestLeaserRenewLease() {
	leaser, del := ts.leaserWithEPHAndLeases()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()
	leases, err := leaser.GetLeases(ctx)
	ts.Require().NoError(err)
	lease := leases[0]
	// should fail if lease was never acquired
	_, ok, err := leaser.RenewLease(ctx, lease.GetPartitionID())
	ts.Require().Error(err)
	ts.Require().False(ok, "shouldn't be ok")

	acquired, ok, err := leaser.AcquireLease(ctx, lease.GetPartitionID())
	ts.Require().NoError(err)
	ts.Require().True(ok, "wasn't able to acquire lease")

	_, ok, err = leaser.RenewLease(ctx, acquired.GetPartitionID())
	ts.NoError(err)
	ts.True(ok, "should have acquired")
}

func (ts *testSuite) TestLeaserRelease() {
	leaser, del := ts.leaserWithEPHAndLeases()
	defer del()

	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()
	leases, err := leaser.GetLeases(ctx)
	ts.Require().NoError(err)

	lease := leases[0]
	acquired, ok, err := leaser.AcquireLease(ctx, lease.GetPartitionID())
	ts.Require().NoError(err)
	ts.Require().True(ok, "should have acquired")
	ts.Equal(1, len(leaser.leases))

	ok, err = leaser.ReleaseLease(ctx, acquired.GetPartitionID())
	ts.Require().NoError(err)
	ts.True(ok, "should have released")
	ts.Equal(0, len(leaser.leases))
}

func (ts *testSuite) leaserWithEPHAndLeases() (*LeaserCheckpointer, func()) {
	leaser, del := ts.leaserWithEPH()

	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()
	for _, partitionID := range leaser.processor.GetPartitionIDs() {
		lease, err := leaser.EnsureLease(ctx, partitionID)
		ts.NoError(err)
		ts.Equal(partitionID, lease.GetPartitionID())
	}

	return leaser, del
}

func (ts *testSuite) leaserWithEPH() (*LeaserCheckpointer, func()) {
	leaser, del := ts.newLeaser()
	hub, delHub := ts.RandomHub()
	delAll := func() {
		delHub()
		del()
	}

	provider, err := aad.NewJWTProvider(aad.JWTProviderWithEnvironmentVars())
	if !ts.NoError(err) {
		delAll()
		ts.FailNow("could not build a new JWT provider from env")
	}

	ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
	defer cancel()
	processor, err := eph.New(ctx, ts.Namespace, *hub.Name, provider, nil, nil)
	if !ts.NoError(err) {
		delAll()
		ts.FailNow("could not create a new eph")
	}
	leaser.SetEventHostProcessor(processor)
	if !ts.NoError(leaser.EnsureStore(ctx)) {
		delAll()
		ts.FailNow("could not ensure store")
	}

	return leaser, delAll
}

func (ts *testSuite) newLeaser() (*LeaserCheckpointer, func()) {
	containerName := strings.ToLower(ts.RandomName("stortest", 4))
	cred, err := NewAADSASCredential(ts.SubscriptionID, ts.ResourceGroupName, ts.AccountName, containerName, AADSASCredentialWithEnvironmentVars())
	ts.Require().NoError(err)
	leaser, err := NewStorageLeaserCheckpointer(cred, ts.AccountName, containerName, ts.Env)
	ts.Require().NoError(err)
	return leaser, func() {
		ctx, cancel := context.WithTimeout(context.Background(), shortTimeout)
		defer cancel()
		if err := leaser.DeleteStore(ctx); err != nil {
			ts.T().Fatal(err)
		}
	}
}
