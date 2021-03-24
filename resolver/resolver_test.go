package resolver_test

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-blockservice"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipldcbor "github.com/ipfs/go-ipld-cbor"
	merkledag "github.com/ipfs/go-merkledag"
	path "github.com/ipfs/go-path"
	"github.com/ipfs/go-path/resolver"
	"github.com/ipfs/go-unixfsnode"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randNode() *merkledag.ProtoNode {
	node := new(merkledag.ProtoNode)
	node.SetData(make([]byte, 32))
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.Read(node.Data())
	return node
}

func TestRecurivePathResolution(t *testing.T) {
	ctx := context.Background()
	bsrv := mockBlockService()

	a := randNode()
	b := randNode()
	c := randNode()

	err := b.AddNodeLink("grandchild", c)
	if err != nil {
		t.Fatal(err)
	}

	err = a.AddNodeLink("child", b)
	if err != nil {
		t.Fatal(err)
	}

	for _, n := range []*merkledag.ProtoNode{a, b, c} {
		err = bsrv.AddBlock(n)
		if err != nil {
			t.Fatal(err)
		}
	}

	aKey := a.Cid()

	segments := []string{aKey.String(), "child", "grandchild"}
	p, err := path.FromSegments("/ipfs/", segments...)
	if err != nil {
		t.Fatal(err)
	}

	resolver := resolver.NewBasicResolver(bsrv)
	node, err := resolver.ResolvePath(ctx, p)
	if err != nil {
		t.Fatal(err)
	}

	uNode, ok := node.(unixfsnode.UnixFSNode)
	require.True(t, ok)
	fd := uNode.FieldData()
	byts, err := fd.Must().AsBytes()
	require.NoError(t, err)

	assert.Equal(t, c.Data(), byts)
	cKey := c.Cid()

	rCid, rest, err := resolver.ResolveToLastNode(ctx, p)
	if err != nil {
		t.Fatal(err)
	}

	if len(rest) != 0 {
		t.Error("expected rest to be empty")
	}

	if rCid.String() != cKey.String() {
		t.Fatal(fmt.Errorf(
			"ResolveToLastNode failed for %s: %s != %s",
			p.String(), rCid.String(), cKey.String()))
	}

	p2, err := path.FromSegments("/ipfs/", aKey.String())
	if err != nil {
		t.Fatal(err)
	}

	rCid, rest, err = resolver.ResolveToLastNode(ctx, p2)
	if err != nil {
		t.Fatal(err)
	}

	if len(rest) != 0 {
		t.Error("expected rest to be empty")
	}

	if rCid.String() != aKey.String() {
		t.Fatal(fmt.Errorf(
			"ResolveToLastNode failed for %s: %s != %s",
			p.String(), rCid.String(), cKey.String()))
	}
}

func TestResolveToLastNode_NoUnnecessaryFetching(t *testing.T) {
	ctx := context.Background()
	bsrv := mockBlockService()

	a := randNode()
	b := randNode()

	err := a.AddNodeLink("child", b)
	require.NoError(t, err)

	err = bsrv.AddBlock(a)
	require.NoError(t, err)

	aKey := a.Cid()

	segments := []string{aKey.String(), "child"}
	p, err := path.FromSegments("/ipfs/", segments...)
	require.NoError(t, err)

	resolver := resolver.NewBasicResolver(bsrv)
	resolvedCID, remainingPath, err := resolver.ResolveToLastNode(ctx, p)
	require.NoError(t, err)

	require.Equal(t, len(remainingPath), 0, "cannot have remaining path")
	require.Equal(t, b.Cid(), resolvedCID)
}

func TestPathRemainder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bsrv := mockBlockService()

	nd, err := ipldcbor.FromJSON(strings.NewReader(`{"foo": {"bar": "baz"}}`), math.MaxUint64, -1)
	require.NoError(t, err)

	err = bsrv.AddBlock(nd)
	require.NoError(t, err)

	resolver := resolver.NewBasicResolver(bsrv)
	rp1, remainder, err := resolver.ResolveToLastNode(ctx, path.FromString(nd.String()+"/foo/bar"))
	require.NoError(t, err)

	assert.Equal(t, nd.Cid(), rp1)
	require.Equal(t, "foo/bar", path.Join(remainder))
}

func mockBlockService() blockservice.BlockService {
	bstore := blockstore.NewBlockstore(dssync.MutexWrap(ds.NewMapDatastore()))
	return blockservice.New(bstore, offline.Exchange(bstore))
}
