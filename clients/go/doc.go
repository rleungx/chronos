// Package chronos provides the application-facing Go client for Chronos.
//
// The intended usage model is:
//
//  1. Create one client bound to one timeline.
//  2. Call AllocateTimestamps during normal operation.
//  3. Close the client when done.
//
// Internal route management stays inside the client. The package ensures the bound
// timeline, fetches the current route, caches it, and refreshes it on stale-route errors.
// Applications should not call internal routing RPCs directly.
package chronos
