// Package kubernetes is the controller's read-only view of the cluster.
//
// Phase 1 observes and never writes, and that is enforced in three independent
// layers rather than by convention:
//
//  1. ClusterReader exposes only reads. The controller holds this interface and
//     never the clientset, so there is no write method in scope for it to call
//     even by mistake.
//
//  2. The REST transport rejects every mutating HTTP method. Even code that got
//     hold of a clientset could not issue a POST, PUT, PATCH, or DELETE — the
//     request fails before it reaches the wire (see NewReadOnlyRESTConfig).
//
//  3. The RBAC shipped in deploy/ grants only get, list, and watch, so the API
//     server would refuse a mutation that somehow escaped the first two.
//
// One of those would be a claim. Three, with the middle one covering the gap
// between "the interface has no write method" and "no write can happen", is
// closer to a guarantee — and the transport layer is what makes the no-write
// property testable without a cluster.
//
// Reads in the reconcile path come from informer caches, so a reconcile issues
// no API calls at all (NFR-04). The exception is metrics.k8s.io, which has no
// watch support and is therefore a live read each interval.
package kubernetes
