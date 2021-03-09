// Package resolver implements utilities for resolving paths within ipfs.
package resolver

import (
	"context"
	"errors"
	"fmt"
	"time"

	path "github.com/ipfs/go-path"

	"github.com/ipfs/go-blockservice"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-fetcher"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log"
	"github.com/ipfs/go-unixfsnode"
	ipldp "github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

var log = logging.Logger("pathresolv")

// ErrNoComponents is used when Paths after a protocol
// do not contain at least one component
var ErrNoComponents = errors.New(
	"path must contain at least one component")

// ErrNoLink is returned when a link is not found in a path
type ErrNoLink struct {
	Name string
	Node cid.Cid
}

// Error implements the Error interface for ErrNoLink with a useful
// human readable message.
func (e ErrNoLink) Error() string {
	return fmt.Sprintf("no link named %q under %s", e.Name, e.Node.String())
}

// ResolveOnce resolves path through a single node
type ResolveOnce func(ctx context.Context, ds ipld.NodeGetter, nd ipld.Node, names []string) (*ipld.Link, []string, error)

// Resolver provides path resolution to IPFS
// It has a pointer to a FetcherConfig, which is uses to resolve nodes.
// TODO: now that this is more modular, try to unify this code with the
//       the resolvers in namesys
type Resolver struct {
	FetchConfig fetcher.FetcherConfig

	ResolveOnce ResolveOnce
}

// NewBasicResolver constructs a new basic resolver.
func NewBasicResolver(bs blockservice.BlockService) *Resolver {
	fc := fetcher.NewFetcherConfig(bs)
	fc.PrototypeChooser = pathFollowingNodeChooser
	return &Resolver{
		FetchConfig: fc,
		ResolveOnce: ResolveSingle,
	}
}

// ResolveToLastNode walks the given path and returns the cid of the last node
// referenced by the path
func (r *Resolver) ResolveToLastNode(ctx context.Context, fpath path.Path) (cid.Cid, []string, error) {
	c, p, err := path.SplitAbsPath(fpath)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	if len(p) == 0 {
		return c, nil, nil
	}

	// create a selector to traverse all path segments but only match the last
	pathSelector, err := pathLeafSelector(p[:len(p)-1])
	if err != nil {
		return cid.Cid{}, nil, err
	}

	// resolve node before last path segment
	nodes, err := r.resolveNodes(ctx, c, pathSelector)
	if err != nil {
		return cid.Cid{}, nil, err
	}
	if len(nodes) < 1 {
		return cid.Cid{}, nil, fmt.Errorf("path %v did not resolve to a node", fpath)
	}
	parent := nodes[len(nodes)-1]

	// find final path segment within node
	nd, err := parent.LookupByString(p[len(p)-1])
	if err != nil {
		return cid.Cid{}, nil, err
	}

	if nd.Kind() != ipldp.Kind_Link {
		return cid.Cid{}, nil, fmt.Errorf("path %v resolves to something other than a cid link: %v", fpath, nd)
	}

	lnk, err := nd.AsLink()
	if err != nil {
		return cid.Cid{}, nil, err
	}

	clnk, ok := lnk.(cidlink.Link)
	if !ok {
		return cid.Cid{}, nil, fmt.Errorf("path %v resolves to something other than a cid link: %v", fpath, lnk)
	}

	return clnk.Cid, nil, nil
}

// ResolvePath fetches the node for given path. It returns the last item
// returned by ResolvePathComponents.
func (r *Resolver) ResolvePath(ctx context.Context, fpath path.Path) (ipldp.Node, error) {
	// validate path
	if err := fpath.IsValid(); err != nil {
		return nil, err
	}

	c, p, err := path.SplitAbsPath(fpath)
	if err != nil {
		return nil, err
	}

	// create a selector to traverse all path segments but only match the last
	pathSelector, err := pathLeafSelector(p)
	if err != nil {
		return nil, err
	}

	nodes, err := r.resolveNodes(ctx, c, pathSelector)
	if err != nil {
		return nil, err
	}
	if len(nodes) < 1 {
		return nil, fmt.Errorf("path %v did not resolve to a node", fpath)
	}
	return nodes[len(nodes)-1], nil
}

// ResolveSingle simply resolves one hop of a path through a graph with no
// extra context (does not opaquely resolve through sharded nodes)
func ResolveSingle(ctx context.Context, ds ipld.NodeGetter, nd ipld.Node, names []string) (*ipld.Link, []string, error) {
	return nd.ResolveLink(names)
}

// ResolvePathComponents fetches the nodes for each segment of the given path.
// It uses the first path component as a hash (key) of the first node, then
// resolves all other components walking the links, with ResolveLinks.
func (r *Resolver) ResolvePathComponents(ctx context.Context, fpath path.Path) ([]ipldp.Node, error) {
	evt := log.EventBegin(ctx, "resolvePathComponents", logging.LoggableMap{"fpath": fpath})
	defer evt.Done()

	// validate path
	if err := fpath.IsValid(); err != nil {
		return nil, err
	}

	c, p, err := path.SplitAbsPath(fpath)
	if err != nil {
		return nil, err
	}

	// create a selector to traverse all path segments but only match the last
	pathSelector, err := pathAllSelector(p)
	if err != nil {
		return nil, err
	}

	return r.resolveNodes(ctx, c, pathSelector)
}

// ResolveLinks iteratively resolves names by walking the link hierarchy.
// Every node is fetched from the fetcher, resolving the next name.
// Returns the list of nodes forming the path, starting with ndd. This list is
// guaranteed never to be empty.
//
// ResolveLinks(nd, []string{"foo", "bar", "baz"})
// would retrieve "baz" in ("bar" in ("foo" in nd.Links).Links).Links
func (r *Resolver) ResolveLinks(ctx context.Context, ndd ipldp.Node, names []string) ([]ipldp.Node, error) {

	evt := log.EventBegin(ctx, "resolveLinks", logging.LoggableMap{"names": names})
	defer evt.Done()

	// create a selector to traverse all path segments but only match the last
	pathSelector, err := pathAllSelector(names)
	if err != nil {
		return nil, err
	}

	// create a new cancellable session
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	session := r.FetchConfig.NewSession(ctx)

	// traverse selector
	nodes := []ipldp.Node{ndd}
	err = session.NodeMatching(ctx, ndd, pathSelector, func(res fetcher.FetchResult) error {
		nodes = append(nodes, res.Node)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return nodes, err
}

func (r *Resolver) resolveNodes(ctx context.Context, c cid.Cid, sel selector.Selector) ([]ipldp.Node, error) {
	// create a new cancellable session
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	session := r.FetchConfig.NewSession(ctx)

	// traverse selector
	nodes := []ipldp.Node{}
	err := fetcher.BlockMatching(ctx, session, cidlink.Link{c}, sel, func(res fetcher.FetchResult) error {
		nodes = append(nodes, res.Node)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

func pathLeafSelector(path []string) (selector.Selector, error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return pathSelector(path, ssb, func(p string, s builder.SelectorSpec) builder.SelectorSpec {
		return ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) { efsb.Insert(p, s) })
	})
}

func pathAllSelector(path []string) (selector.Selector, error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return pathSelector(path, ssb, func(p string, s builder.SelectorSpec) builder.SelectorSpec {
		return ssb.ExploreUnion(
			ssb.Matcher(),
			ssb.ExploreFields(func(efsb builder.ExploreFieldsSpecBuilder) { efsb.Insert(p, s) }),
		)
	})
}

func pathSelector(path []string, ssb builder.SelectorSpecBuilder, reduce func(string, builder.SelectorSpec) builder.SelectorSpec) (selector.Selector, error) {
	spec := ssb.Matcher()
	for i := len(path) - 1; i >= 0; i-- {
		spec = reduce(path[i], spec)
	}
	return spec.Selector()
}

func pathFollowingNodeChooser(lnk ipldp.Link, lnkCtx ipldp.LinkContext) (ipldp.NodePrototype, error) {
	c, ok := lnk.(cidlink.Link)
	if ok {
		if c.Cid.Prefix().Codec == 0x70 {
			return unixfsnode.Type.UnixFSNode, nil
		}
	}
	return fetcher.DefaultPrototypeChooser(lnk, lnkCtx)
}
