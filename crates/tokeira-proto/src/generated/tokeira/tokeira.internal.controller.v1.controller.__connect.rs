///Shorthand for `OwnedView<RuntimeMembershipRequestView<'static>>`.
pub type OwnedRuntimeMembershipRequestView = ::buffa::view::OwnedView<
    __buffa::view::RuntimeMembershipRequestView<'static>,
>;
///Shorthand for `OwnedView<ControllerDirectiveView<'static>>`.
pub type OwnedControllerDirectiveView = ::buffa::view::OwnedView<
    __buffa::view::ControllerDirectiveView<'static>,
>;
///Shorthand for `OwnedView<SubscribeRoutingRequestView<'static>>`.
pub type OwnedSubscribeRoutingRequestView = ::buffa::view::OwnedView<
    __buffa::view::SubscribeRoutingRequestView<'static>,
>;
///Shorthand for `OwnedView<RoutingUpdateView<'static>>`.
pub type OwnedRoutingUpdateView = ::buffa::view::OwnedView<
    __buffa::view::RoutingUpdateView<'static>,
>;
///Shorthand for `OwnedView<RefreshBundleRequestView<'static>>`.
pub type OwnedRefreshBundleRequestView = ::buffa::view::OwnedView<
    __buffa::view::RefreshBundleRequestView<'static>,
>;
///Shorthand for `OwnedView<RefreshBundleResponseView<'static>>`.
pub type OwnedRefreshBundleResponseView = ::buffa::view::OwnedView<
    __buffa::view::RefreshBundleResponseView<'static>,
>;
///Shorthand for `OwnedView<NominateRequestView<'static>>`.
pub type OwnedNominateRequestView = ::buffa::view::OwnedView<
    __buffa::view::NominateRequestView<'static>,
>;
///Shorthand for `OwnedView<NominateResponseView<'static>>`.
pub type OwnedNominateResponseView = ::buffa::view::OwnedView<
    __buffa::view::NominateResponseView<'static>,
>;
///Shorthand for `OwnedView<MarkDrainingRequestView<'static>>`.
pub type OwnedMarkDrainingRequestView = ::buffa::view::OwnedView<
    __buffa::view::MarkDrainingRequestView<'static>,
>;
///Shorthand for `OwnedView<MarkDrainingResponseView<'static>>`.
pub type OwnedMarkDrainingResponseView = ::buffa::view::OwnedView<
    __buffa::view::MarkDrainingResponseView<'static>,
>;
///Shorthand for `OwnedView<DescribeNodeDrainRequestView<'static>>`.
pub type OwnedDescribeNodeDrainRequestView = ::buffa::view::OwnedView<
    __buffa::view::DescribeNodeDrainRequestView<'static>,
>;
///Shorthand for `OwnedView<DescribeNodeDrainResponseView<'static>>`.
pub type OwnedDescribeNodeDrainResponseView = ::buffa::view::OwnedView<
    __buffa::view::DescribeNodeDrainResponseView<'static>,
>;
impl ::connectrpc::Encodable<ControllerDirective>
for __buffa::view::ControllerDirectiveView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<ControllerDirective>
for ::buffa::view::OwnedView<__buffa::view::ControllerDirectiveView<'static>> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
impl ::connectrpc::Encodable<RoutingUpdate> for __buffa::view::RoutingUpdateView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<RoutingUpdate>
for ::buffa::view::OwnedView<__buffa::view::RoutingUpdateView<'static>> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
impl ::connectrpc::Encodable<RefreshBundleResponse>
for __buffa::view::RefreshBundleResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<RefreshBundleResponse>
for ::buffa::view::OwnedView<__buffa::view::RefreshBundleResponseView<'static>> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
impl ::connectrpc::Encodable<NominateResponse>
for __buffa::view::NominateResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<NominateResponse>
for ::buffa::view::OwnedView<__buffa::view::NominateResponseView<'static>> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
impl ::connectrpc::Encodable<MarkDrainingResponse>
for __buffa::view::MarkDrainingResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<MarkDrainingResponse>
for ::buffa::view::OwnedView<__buffa::view::MarkDrainingResponseView<'static>> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
impl ::connectrpc::Encodable<DescribeNodeDrainResponse>
for __buffa::view::DescribeNodeDrainResponseView<'_> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(self, codec)
    }
}
impl ::connectrpc::Encodable<DescribeNodeDrainResponse>
for ::buffa::view::OwnedView<__buffa::view::DescribeNodeDrainResponseView<'static>> {
    fn encode(
        &self,
        codec: ::connectrpc::CodecFormat,
    ) -> ::std::result::Result<::buffa::bytes::Bytes, ::connectrpc::ConnectError> {
        ::connectrpc::__codegen::encode_view_body(&**self, codec)
    }
}
/// Full service name for this service.
pub const PLACEMENT_CONTROLLER_SERVICE_NAME: &str = "tokeira.internal.controller.v1.PlacementController";
/// Static [`Spec`](::connectrpc::Spec) for the server-side `RuntimeMembership` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const PLACEMENT_CONTROLLER_RUNTIME_MEMBERSHIP_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/tokeira.internal.controller.v1.PlacementController/RuntimeMembership",
        ::connectrpc::StreamType::BidiStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `SubscribeRouting` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const PLACEMENT_CONTROLLER_SUBSCRIBE_ROUTING_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/tokeira.internal.controller.v1.PlacementController/SubscribeRouting",
        ::connectrpc::StreamType::ServerStream,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `RefreshBundle` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const PLACEMENT_CONTROLLER_REFRESH_BUNDLE_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/tokeira.internal.controller.v1.PlacementController/RefreshBundle",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `NominateScaleInCandidates` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const PLACEMENT_CONTROLLER_NOMINATE_SCALE_IN_CANDIDATES_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/tokeira.internal.controller.v1.PlacementController/NominateScaleInCandidates",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `MarkNodeDraining` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const PLACEMENT_CONTROLLER_MARK_NODE_DRAINING_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/tokeira.internal.controller.v1.PlacementController/MarkNodeDraining",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Static [`Spec`](::connectrpc::Spec) for the server-side `DescribeNodeDrain` RPC.
///
/// The dispatcher surfaces this on
/// [`RequestContext::spec`](::connectrpc::RequestContext::spec).
pub const PLACEMENT_CONTROLLER_DESCRIBE_NODE_DRAIN_SPEC: ::connectrpc::Spec = ::connectrpc::Spec::server(
        "/tokeira.internal.controller.v1.PlacementController/DescribeNodeDrain",
        ::connectrpc::StreamType::Unary,
    )
    .with_idempotency_level(::connectrpc::IdempotencyLevel::Unknown);
/// Server trait for PlacementController.
///
/// # Implementing handlers
///
/// Handlers receive requests as `OwnedFooView` (an alias for
/// `OwnedView<FooView<'static>>`), which gives zero-copy borrowed access
/// to fields (e.g. `request.name` is a `&str` into the decoded buffer).
/// The view can be held across `.await` points. When two RPC types in
/// the same package would alias to the same `Owned<…>View` name (e.g.
/// a local message plus an imported one with the same short name), the
/// alias is suppressed for both and the request type is spelled as
/// `OwnedView<…View<'static>>` directly in the trait signature.
///
/// Implement methods with plain `async fn`; the returned future satisfies
/// the `Send` bound automatically. See the
/// [buffa user guide](https://github.com/anthropics/buffa/blob/main/docs/guide.md#ownedview-in-async-trait-implementations)
/// for zero-copy access patterns and when `to_owned_message()` is needed.
///
/// The `impl Encodable<Out>` return bound accepts the owned `Out`, the
/// generated `OutView<'_>` / `OwnedOutView`,
/// [`MaybeBorrowed`](::connectrpc::MaybeBorrowed), or
/// [`PreEncoded`](::connectrpc::PreEncoded) for handlers that encode a
/// non-`'static` view internally and pass the bytes across the handler
/// boundary. View bodies are not emitted for output types mapped via
/// `extern_path` (the impl would be an orphan); return owned for
/// WKT/extern outputs.
///
/// Server-streaming and bidi-streaming methods return
/// `ServiceStream<impl Encodable<Out> + Send + use<Self>>`. The
/// `use<Self>` precise-capturing clause excludes `&self`'s lifetime
/// (unary methods use `use<'a, Self>` and may borrow), so stream items
/// must be `'static`. To stream view-encoded data, encode each item
/// inside the stream body and yield
/// [`PreEncoded`](::connectrpc::PreEncoded) — see its `# Streaming
/// example` doc.
#[allow(clippy::type_complexity)]
pub trait PlacementController: Send + Sync + 'static {
    /// Handle the RuntimeMembership RPC.
    fn runtime_membership(
        &self,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::ServiceStream<OwnedRuntimeMembershipRequestView>,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<ControllerDirective> + Send + use<Self>,
            >,
        >,
    > + Send;
    /// Handle the SubscribeRouting RPC.
    fn subscribe_routing(
        &self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedSubscribeRoutingRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            ::connectrpc::ServiceStream<
                impl ::connectrpc::Encodable<RoutingUpdate> + Send + use<Self>,
            >,
        >,
    > + Send;
    /// Handle the RefreshBundle RPC.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    fn refresh_bundle<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedRefreshBundleRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<RefreshBundleResponse> + Send + use<'a, Self>,
        >,
    > + Send;
    /// Handle the NominateScaleInCandidates RPC.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    fn nominate_scale_in_candidates<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedNominateRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<NominateResponse> + Send + use<'a, Self>,
        >,
    > + Send;
    /// Handle the MarkNodeDraining RPC.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    fn mark_node_draining<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedMarkDrainingRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<MarkDrainingResponse> + Send + use<'a, Self>,
        >,
    > + Send;
    /// Handle the DescribeNodeDrain RPC.
    ///
    /// `'a` lets the response body borrow from `&self` (e.g. server-resident state).
    fn describe_node_drain<'a>(
        &'a self,
        ctx: ::connectrpc::RequestContext,
        request: OwnedDescribeNodeDrainRequestView,
    ) -> impl ::std::future::Future<
        Output = ::connectrpc::ServiceResult<
            impl ::connectrpc::Encodable<
                DescribeNodeDrainResponse,
            > + Send + use<'a, Self>,
        >,
    > + Send;
}
/// Extension trait for registering a service implementation with a Router.
///
/// This trait is automatically implemented for all types that implement the service trait.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// let service = Arc::new(MyServiceImpl);
/// let router = service.register(Router::new());
/// ```
pub trait PlacementControllerExt: PlacementController {
    /// Register this service implementation with a Router.
    ///
    /// Takes ownership of the `Arc<Self>` and returns a new Router with
    /// this service's methods registered.
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router;
}
impl<S: PlacementController> PlacementControllerExt for S {
    fn register(
        self: ::std::sync::Arc<Self>,
        router: ::connectrpc::Router,
    ) -> ::connectrpc::Router {
        router
            .route_view_bidi_stream::<
                _,
                _,
                ControllerDirective,
            >(
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "RuntimeMembership",
                ::connectrpc::view_bidi_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |ctx, req| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move { svc.runtime_membership(ctx, req).await }
                    }
                }),
            )
            .with_spec(PLACEMENT_CONTROLLER_RUNTIME_MEMBERSHIP_SPEC)
            .route_view_server_stream::<
                _,
                _,
                RoutingUpdate,
            >(
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "SubscribeRouting",
                ::connectrpc::view_streaming_handler_fn({
                    let svc = ::std::sync::Arc::clone(&self);
                    move |ctx, req| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move { svc.subscribe_routing(ctx, req).await }
                    }
                }),
            )
            .with_spec(PLACEMENT_CONTROLLER_SUBSCRIBE_ROUTING_SPEC)
            .route_view(
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "RefreshBundle",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            svc.refresh_bundle(ctx, req)
                                .await?
                                .encode::<RefreshBundleResponse>(format)
                        }
                    })
                },
            )
            .with_spec(PLACEMENT_CONTROLLER_REFRESH_BUNDLE_SPEC)
            .route_view(
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "NominateScaleInCandidates",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            svc.nominate_scale_in_candidates(ctx, req)
                                .await?
                                .encode::<NominateResponse>(format)
                        }
                    })
                },
            )
            .with_spec(PLACEMENT_CONTROLLER_NOMINATE_SCALE_IN_CANDIDATES_SPEC)
            .route_view(
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "MarkNodeDraining",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            svc.mark_node_draining(ctx, req)
                                .await?
                                .encode::<MarkDrainingResponse>(format)
                        }
                    })
                },
            )
            .with_spec(PLACEMENT_CONTROLLER_MARK_NODE_DRAINING_SPEC)
            .route_view(
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "DescribeNodeDrain",
                {
                    let svc = ::std::sync::Arc::clone(&self);
                    ::connectrpc::view_handler_fn(move |ctx, req, format| {
                        let svc = ::std::sync::Arc::clone(&svc);
                        async move {
                            svc.describe_node_drain(ctx, req)
                                .await?
                                .encode::<DescribeNodeDrainResponse>(format)
                        }
                    })
                },
            )
            .with_spec(PLACEMENT_CONTROLLER_DESCRIBE_NODE_DRAIN_SPEC)
    }
}
/// Monomorphic dispatcher for `PlacementController`.
///
/// Unlike `.register(Router)` which type-erases each method into an `Arc<dyn ErasedHandler>` stored in a `HashMap`, this struct dispatches via a compile-time `match` on method name: no vtable, no hash lookup.
///
/// # Example
///
/// ```rust,ignore
/// use connectrpc::ConnectRpcService;
///
/// let server = PlacementControllerServer::new(MyImpl);
/// let service = ConnectRpcService::new(server);
/// // hand `service` to axum/hyper as a fallback_service
/// ```
pub struct PlacementControllerServer<T> {
    inner: ::std::sync::Arc<T>,
}
impl<T: PlacementController> PlacementControllerServer<T> {
    /// Wrap a service implementation in a monomorphic dispatcher.
    pub fn new(service: T) -> Self {
        Self {
            inner: ::std::sync::Arc::new(service),
        }
    }
    /// Wrap an already-`Arc`'d service implementation.
    pub fn from_arc(inner: ::std::sync::Arc<T>) -> Self {
        Self { inner }
    }
}
impl<T> Clone for PlacementControllerServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: ::std::sync::Arc::clone(&self.inner),
        }
    }
}
impl<T: PlacementController> ::connectrpc::Dispatcher for PlacementControllerServer<T> {
    #[inline]
    fn lookup(
        &self,
        path: &str,
    ) -> Option<::connectrpc::dispatcher::codegen::MethodDescriptor> {
        let method = path
            .strip_prefix("tokeira.internal.controller.v1.PlacementController/")?;
        match method {
            "RuntimeMembership" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::bidi_streaming()
                        .with_spec(PLACEMENT_CONTROLLER_RUNTIME_MEMBERSHIP_SPEC),
                )
            }
            "SubscribeRouting" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::server_streaming()
                        .with_spec(PLACEMENT_CONTROLLER_SUBSCRIBE_ROUTING_SPEC),
                )
            }
            "RefreshBundle" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(PLACEMENT_CONTROLLER_REFRESH_BUNDLE_SPEC),
                )
            }
            "NominateScaleInCandidates" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(
                            PLACEMENT_CONTROLLER_NOMINATE_SCALE_IN_CANDIDATES_SPEC,
                        ),
                )
            }
            "MarkNodeDraining" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(PLACEMENT_CONTROLLER_MARK_NODE_DRAINING_SPEC),
                )
            }
            "DescribeNodeDrain" => {
                Some(
                    ::connectrpc::dispatcher::codegen::MethodDescriptor::unary(false)
                        .with_spec(PLACEMENT_CONTROLLER_DESCRIBE_NODE_DRAIN_SPEC),
                )
            }
            _ => None,
        }
    }
    fn call_unary(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::connectrpc::Payload,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path
            .strip_prefix("tokeira.internal.controller.v1.PlacementController/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "RefreshBundle" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        __buffa::view::RefreshBundleRequestView,
                    >(request.encoded()?, format)?;
                    svc.refresh_bundle(ctx, req)
                        .await?
                        .encode::<RefreshBundleResponse>(format)
                })
            }
            "NominateScaleInCandidates" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        __buffa::view::NominateRequestView,
                    >(request.encoded()?, format)?;
                    svc.nominate_scale_in_candidates(ctx, req)
                        .await?
                        .encode::<NominateResponse>(format)
                })
            }
            "MarkNodeDraining" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        __buffa::view::MarkDrainingRequestView,
                    >(request.encoded()?, format)?;
                    svc.mark_node_draining(ctx, req)
                        .await?
                        .encode::<MarkDrainingResponse>(format)
                })
            }
            "DescribeNodeDrain" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        __buffa::view::DescribeNodeDrainRequestView,
                    >(request.encoded()?, format)?;
                    svc.describe_node_drain(ctx, req)
                        .await?
                        .encode::<DescribeNodeDrainResponse>(format)
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_server_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        request: ::buffa::bytes::Bytes,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path
            .strip_prefix("tokeira.internal.controller.v1.PlacementController/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &request, &format);
        match method {
            "SubscribeRouting" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req = ::connectrpc::dispatcher::codegen::decode_request_view::<
                        __buffa::view::SubscribeRoutingRequestView,
                    >(request, format)?;
                    let resp = svc.subscribe_routing(ctx, req).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                RoutingUpdate,
                                _,
                                _,
                            >(s, format)),
                    )
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
    fn call_client_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::UnaryResult {
        let Some(method) = path
            .strip_prefix("tokeira.internal.controller.v1.PlacementController/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_unary(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            _ => ::connectrpc::dispatcher::codegen::unimplemented_unary(path),
        }
    }
    fn call_bidi_streaming(
        &self,
        path: &str,
        ctx: ::connectrpc::RequestContext,
        requests: ::connectrpc::dispatcher::codegen::RequestStream,
        format: ::connectrpc::CodecFormat,
    ) -> ::connectrpc::dispatcher::codegen::StreamingResult {
        let Some(method) = path
            .strip_prefix("tokeira.internal.controller.v1.PlacementController/") else {
            return ::connectrpc::dispatcher::codegen::unimplemented_streaming(path);
        };
        let _ = (&ctx, &requests, &format);
        match method {
            "RuntimeMembership" => {
                let svc = ::std::sync::Arc::clone(&self.inner);
                Box::pin(async move {
                    let req_stream = ::connectrpc::dispatcher::codegen::decode_view_request_stream::<
                        __buffa::view::RuntimeMembershipRequestView,
                    >(requests, format);
                    let resp = svc.runtime_membership(ctx, req_stream).await?;
                    Ok(
                        resp
                            .map_body(|s| ::connectrpc::dispatcher::codegen::encode_response_stream::<
                                ControllerDirective,
                                _,
                                _,
                            >(s, format)),
                    )
                })
            }
            _ => ::connectrpc::dispatcher::codegen::unimplemented_streaming(path),
        }
    }
}
/// Client for this service.
///
/// Generic over `T: ClientTransport`. For **gRPC** (HTTP/2), use
/// `Http2Connection` — it has honest `poll_ready` and composes with
/// `tower::balance` for multi-connection load balancing. For **Connect
/// over HTTP/1.1** (or unknown protocol), use `HttpClient`.
///
/// # Example (gRPC / HTTP/2)
///
/// ```rust,ignore
/// use connectrpc::client::{Http2Connection, ClientConfig};
/// use connectrpc::Protocol;
///
/// let uri: http::Uri = "http://localhost:8080".parse()?;
/// let conn = Http2Connection::connect_plaintext(uri.clone()).await?.shared(1024);
/// let config = ClientConfig::new(uri).with_protocol(Protocol::Grpc);
///
/// let client = PlacementControllerClient::new(conn, config);
/// let response = client.runtime_membership(request).await?;
/// ```
///
/// # Example (Connect / HTTP/1.1 or ALPN)
///
/// ```rust,ignore
/// use connectrpc::client::{HttpClient, ClientConfig};
///
/// let http = HttpClient::plaintext();  // cleartext http:// only
/// let config = ClientConfig::new("http://localhost:8080".parse()?);
///
/// let client = PlacementControllerClient::new(http, config);
/// let response = client.runtime_membership(request).await?;
/// ```
///
/// # Working with the response
///
/// Unary calls return [`UnaryResponse<OwnedView<FooView>>`](::connectrpc::client::UnaryResponse).
/// The `OwnedView` derefs to the view, so field access is zero-copy:
///
/// ```rust,ignore
/// let resp = client.runtime_membership(request).await?.into_view();
/// let name: &str = resp.name;  // borrow into the response buffer
/// ```
///
/// If you need the owned struct (e.g. to store or pass by value), use
/// [`into_owned()`](::connectrpc::client::UnaryResponse::into_owned):
///
/// ```rust,ignore
/// let owned = client.runtime_membership(request).await?.into_owned();
/// ```
#[derive(Clone)]
pub struct PlacementControllerClient<T> {
    transport: T,
    config: ::connectrpc::client::ClientConfig,
}
impl<T> PlacementControllerClient<T>
where
    T: ::connectrpc::client::ClientTransport,
    <T::ResponseBody as ::http_body::Body>::Error: ::std::fmt::Display,
{
    /// Create a new client with the given transport and configuration.
    pub fn new(transport: T, config: ::connectrpc::client::ClientConfig) -> Self {
        Self { transport, config }
    }
    /// Get the client configuration.
    pub fn config(&self) -> &::connectrpc::client::ClientConfig {
        &self.config
    }
    /// Get a mutable reference to the client configuration.
    pub fn config_mut(&mut self) -> &mut ::connectrpc::client::ClientConfig {
        &mut self.config
    }
    /// Call the RuntimeMembership RPC. Sends a request to /tokeira.internal.controller.v1.PlacementController/RuntimeMembership.
    pub async fn runtime_membership(
        &self,
    ) -> Result<
        ::connectrpc::client::BidiStream<
            T::ResponseBody,
            RuntimeMembershipRequest,
            __buffa::view::ControllerDirectiveView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.runtime_membership_with_options(
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the RuntimeMembership RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn runtime_membership_with_options(
        &self,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::BidiStream<
            T::ResponseBody,
            RuntimeMembershipRequest,
            __buffa::view::ControllerDirectiveView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_bidi_stream(
                &self.transport,
                &self.config,
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "RuntimeMembership",
                options,
            )
            .await
    }
    /// Call the SubscribeRouting RPC. Sends a request to /tokeira.internal.controller.v1.PlacementController/SubscribeRouting.
    pub async fn subscribe_routing(
        &self,
        request: SubscribeRoutingRequest,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            __buffa::view::RoutingUpdateView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.subscribe_routing_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the SubscribeRouting RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn subscribe_routing_with_options(
        &self,
        request: SubscribeRoutingRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::ServerStream<
            T::ResponseBody,
            __buffa::view::RoutingUpdateView<'static>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_server_stream(
                &self.transport,
                &self.config,
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "SubscribeRouting",
                request,
                options,
            )
            .await
    }
    /// Call the RefreshBundle RPC. Sends a request to /tokeira.internal.controller.v1.PlacementController/RefreshBundle.
    pub async fn refresh_bundle(
        &self,
        request: RefreshBundleRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::RefreshBundleResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.refresh_bundle_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the RefreshBundle RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn refresh_bundle_with_options(
        &self,
        request: RefreshBundleRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::RefreshBundleResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "RefreshBundle",
                request,
                options,
            )
            .await
    }
    /// Call the NominateScaleInCandidates RPC. Sends a request to /tokeira.internal.controller.v1.PlacementController/NominateScaleInCandidates.
    pub async fn nominate_scale_in_candidates(
        &self,
        request: NominateRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::NominateResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.nominate_scale_in_candidates_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the NominateScaleInCandidates RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn nominate_scale_in_candidates_with_options(
        &self,
        request: NominateRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::NominateResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "NominateScaleInCandidates",
                request,
                options,
            )
            .await
    }
    /// Call the MarkNodeDraining RPC. Sends a request to /tokeira.internal.controller.v1.PlacementController/MarkNodeDraining.
    pub async fn mark_node_draining(
        &self,
        request: MarkDrainingRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::MarkDrainingResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        self.mark_node_draining_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the MarkNodeDraining RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn mark_node_draining_with_options(
        &self,
        request: MarkDrainingRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<__buffa::view::MarkDrainingResponseView<'static>>,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "MarkNodeDraining",
                request,
                options,
            )
            .await
    }
    /// Call the DescribeNodeDrain RPC. Sends a request to /tokeira.internal.controller.v1.PlacementController/DescribeNodeDrain.
    pub async fn describe_node_drain(
        &self,
        request: DescribeNodeDrainRequest,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                __buffa::view::DescribeNodeDrainResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        self.describe_node_drain_with_options(
                request,
                ::connectrpc::client::CallOptions::default(),
            )
            .await
    }
    /// Call the DescribeNodeDrain RPC with explicit per-call options. Options override [`ClientConfig`](::connectrpc::client::ClientConfig) defaults.
    pub async fn describe_node_drain_with_options(
        &self,
        request: DescribeNodeDrainRequest,
        options: ::connectrpc::client::CallOptions,
    ) -> Result<
        ::connectrpc::client::UnaryResponse<
            ::buffa::view::OwnedView<
                __buffa::view::DescribeNodeDrainResponseView<'static>,
            >,
        >,
        ::connectrpc::ConnectError,
    > {
        ::connectrpc::client::call_unary(
                &self.transport,
                &self.config,
                PLACEMENT_CONTROLLER_SERVICE_NAME,
                "DescribeNodeDrain",
                request,
                options,
            )
            .await
    }
}
