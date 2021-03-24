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

// ResolveToLastNode walks the given path and returns the cid of the last block
// referenced by the path, and the path segments to traverse from the final block boundary to the final node
// within the block.
func (r *Resolver) ResolveToLastNode(ctx context.Context, fpath path.Path) (cid.Cid, []string, error) {
	c, p, err := path.SplitAbsPath(fpath)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	if len(p) == 0 {
		return c, nil, nil
	}

	// create a selector to traverse and match all path segments
	pathSelector, err := pathAllSelector(p[:len(p)-1])
	if err != nil {
		return cid.Cid{}, nil, err
	}

	// resolve node before last path segment
	nodes, lastCid, depth, err := r.resolveNodes(ctx, c, pathSelector)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	if len(nodes) < 1 {
		return cid.Cid{}, nil, fmt.Errorf("path %v did not resolve to a node", fpath)
	}

	parent := nodes[len(nodes)-1]
	lastSegment := p[len(p)-1]

	// find final path segment within node
	nd, err := parent.LookupByString(lastSegment)
	if err != nil {
		return cid.Cid{}, nil, err
	}

	// if last node is not a link, just return it's cid, add path to remainder and return
	if nd.Kind() != ipldp.Kind_Link {
		// return the cid and the remainder of the path
		return lastCid, p[len(p)-depth-1:], nil
	}

	lnk, err := nd.AsLink()
	if err != nil {
		return cid.Cid{}, nil, err
	}

	clnk, ok := lnk.(cidlink.Link)
	if !ok {
		return cid.Cid{}, nil, fmt.Errorf("path %v resolves to a link that is not a cid link: %v", fpath, lnk)
	}

	return clnk.Cid, []string{}, nil
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

	nodes, _, _, err := r.resolveNodes(ctx, c, pathSelector)
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

	nodes, _, _, err := r.resolveNodes(ctx, c, pathSelector)
	return nodes, err
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

// Finds nodes matching the selector starting with a cid. Returns the matched nodes, the cid of the block containing
// the last node, and the depth of the last node within its block (root is depth 0).
func (r *Resolver) resolveNodes(ctx context.Context, c cid.Cid, sel selector.Selector) ([]ipldp.Node, cid.Cid, int, error) {
	// create a new cancellable session
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	session := r.FetchConfig.NewSession(ctx)

	// traverse selector
	lastLink := cid.Undef
	depth := 0
	nodes := []ipldp.Node{}
	err := fetcher.BlockMatching(ctx, session, cidlink.Link{c}, sel, func(res fetcher.FetchResult) error {
		if res.LastBlockLink == nil {
			res.LastBlockLink = cidlink.Link{c}
		}
		cidLnk, ok := res.LastBlockLink.(cidlink.Link)
		if !ok {
			return fmt.Errorf("link is not a cidlink: %v", cidLnk)
		}

		// if we hit a block boundary
		if !lastLink.Equals(cidLnk.Cid) {
			depth = 0
			lastLink = cidLnk.Cid
		} else {
			depth++
		}

		nodes = append(nodes, res.Node)
		return nil
	})
	if err != nil {
		return nil, cid.Undef, 0, err
	}

	return nodes, lastLink, depth, nil
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
