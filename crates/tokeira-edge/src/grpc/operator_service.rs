use tonic::{Request, Response, Status};

use tokeira_projection::{STANDARD_SEARCH_ATTRIBUTES, SearchAttrType};
use tokeira_proto::{
    enums::IndexedValueType,
    operatorservice::{
        self,
        operator_service_server::{
            OperatorService as OperatorServiceGrpcApi, OperatorServiceServer,
        },
    },
};
use tonic::codec::CompressionEncoding;

use crate::{
    grpc::metadata::metadata_to_header_map,
    operator_service::{OperatorService, SearchAttributeDefinition},
};

#[derive(Clone)]
pub struct OperatorServiceGrpc {
    inner: OperatorService,
}

impl OperatorServiceGrpc {
    pub fn new(inner: OperatorService) -> Self {
        Self { inner }
    }

    pub fn into_service(self) -> OperatorServiceServer<Self> {
        // Negotiate gzip for parity with the SDKs (see WorkflowServiceGrpc).
        OperatorServiceServer::new(self)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Gzip)
    }
}

#[tonic::async_trait]
impl OperatorServiceGrpcApi for OperatorServiceGrpc {
    async fn add_search_attributes(
        &self,
        request: Request<operatorservice::AddSearchAttributesRequest>,
    ) -> Result<Response<operatorservice::AddSearchAttributesResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();

        for (name, attr_type) in req.search_attributes {
            let attr = SearchAttributeDefinition {
                name,
                attr_type: indexed_value_type_to_edge(attr_type)?,
            };
            self.inner
                .upsert_search_attribute(&headers, &req.namespace, attr)
                .await?;
        }

        Ok(Response::new(
            operatorservice::AddSearchAttributesResponse {},
        ))
    }

    async fn remove_search_attributes(
        &self,
        _request: Request<operatorservice::RemoveSearchAttributesRequest>,
    ) -> Result<Response<operatorservice::RemoveSearchAttributesResponse>, Status> {
        Err(Status::unimplemented("remove_search_attributes"))
    }

    async fn list_search_attributes(
        &self,
        request: Request<operatorservice::ListSearchAttributesRequest>,
    ) -> Result<Response<operatorservice::ListSearchAttributesResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();
        let namespace = if req.namespace.is_empty() {
            None
        } else {
            Some(req.namespace.as_str())
        };
        let attrs = self
            .inner
            .list_search_attributes(&headers, namespace)
            .await?;

        Ok(Response::new(
            operatorservice::ListSearchAttributesResponse {
                system_attributes: STANDARD_SEARCH_ATTRIBUTES
                    .iter()
                    .map(|attr| {
                        (
                            attr.name.to_owned(),
                            indexed_value_type_from_projection(attr.attr_type) as i32,
                        )
                    })
                    .collect(),
                custom_attributes: attrs
                    .into_iter()
                    .map(|attr| {
                        Ok((
                            attr.name,
                            indexed_value_type_from_edge(&attr.attr_type)? as i32,
                        ))
                    })
                    .collect::<Result<_, Status>>()?,
                storage_schema: Default::default(),
            },
        ))
    }

    async fn delete_namespace(
        &self,
        _request: Request<operatorservice::DeleteNamespaceRequest>,
    ) -> Result<Response<operatorservice::DeleteNamespaceResponse>, Status> {
        Err(Status::unimplemented("delete_namespace"))
    }

    async fn add_or_update_remote_cluster(
        &self,
        _request: Request<operatorservice::AddOrUpdateRemoteClusterRequest>,
    ) -> Result<Response<operatorservice::AddOrUpdateRemoteClusterResponse>, Status> {
        Err(Status::unimplemented("add_or_update_remote_cluster"))
    }

    async fn remove_remote_cluster(
        &self,
        _request: Request<operatorservice::RemoveRemoteClusterRequest>,
    ) -> Result<Response<operatorservice::RemoveRemoteClusterResponse>, Status> {
        Err(Status::unimplemented("remove_remote_cluster"))
    }

    async fn list_clusters(
        &self,
        _request: Request<operatorservice::ListClustersRequest>,
    ) -> Result<Response<operatorservice::ListClustersResponse>, Status> {
        Err(Status::unimplemented("list_clusters"))
    }

    async fn get_nexus_endpoint(
        &self,
        request: Request<operatorservice::GetNexusEndpointRequest>,
    ) -> Result<Response<operatorservice::GetNexusEndpointResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let resp = self
            .inner
            .get_nexus_endpoint(&headers, request.into_inner())
            .await?;
        Ok(Response::new(resp))
    }

    async fn create_nexus_endpoint(
        &self,
        request: Request<operatorservice::CreateNexusEndpointRequest>,
    ) -> Result<Response<operatorservice::CreateNexusEndpointResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let resp = self
            .inner
            .create_nexus_endpoint(&headers, request.into_inner())
            .await?;
        Ok(Response::new(resp))
    }

    async fn update_nexus_endpoint(
        &self,
        request: Request<operatorservice::UpdateNexusEndpointRequest>,
    ) -> Result<Response<operatorservice::UpdateNexusEndpointResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let resp = self
            .inner
            .update_nexus_endpoint(&headers, request.into_inner())
            .await?;
        Ok(Response::new(resp))
    }

    async fn delete_nexus_endpoint(
        &self,
        request: Request<operatorservice::DeleteNexusEndpointRequest>,
    ) -> Result<Response<operatorservice::DeleteNexusEndpointResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let resp = self
            .inner
            .delete_nexus_endpoint(&headers, request.into_inner())
            .await?;
        Ok(Response::new(resp))
    }

    async fn list_nexus_endpoints(
        &self,
        request: Request<operatorservice::ListNexusEndpointsRequest>,
    ) -> Result<Response<operatorservice::ListNexusEndpointsResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let resp = self
            .inner
            .list_nexus_endpoints(&headers, request.into_inner())
            .await?;
        Ok(Response::new(resp))
    }
}

fn indexed_value_type_from_projection(value: SearchAttrType) -> IndexedValueType {
    match value {
        SearchAttrType::Keyword => IndexedValueType::Keyword,
        SearchAttrType::KeywordList => IndexedValueType::KeywordList,
        SearchAttrType::Int => IndexedValueType::Int,
        SearchAttrType::Bool => IndexedValueType::Bool,
        SearchAttrType::Double => IndexedValueType::Double,
        SearchAttrType::Datetime => IndexedValueType::Datetime,
        SearchAttrType::Text => IndexedValueType::Text,
    }
}

fn indexed_value_type_to_edge(value: i32) -> Result<String, Status> {
    let value = IndexedValueType::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown IndexedValueType: {value}")))?;

    let name = match value {
        IndexedValueType::Unspecified => {
            return Err(Status::invalid_argument("unspecified IndexedValueType"));
        }
        IndexedValueType::Keyword => "keyword",
        IndexedValueType::KeywordList => "keyword_list",
        IndexedValueType::Int => "int",
        IndexedValueType::Bool => "bool",
        IndexedValueType::Double => "double",
        IndexedValueType::Datetime => "datetime",
        IndexedValueType::Text => "text",
    };

    Ok(name.to_string())
}

fn indexed_value_type_from_edge(value: &str) -> Result<IndexedValueType, Status> {
    match value {
        "keyword" | "INDEXED_VALUE_TYPE_KEYWORD" => Ok(IndexedValueType::Keyword),
        "keyword_list" | "INDEXED_VALUE_TYPE_KEYWORD_LIST" => Ok(IndexedValueType::KeywordList),
        "int" | "INDEXED_VALUE_TYPE_INT" => Ok(IndexedValueType::Int),
        "bool" | "INDEXED_VALUE_TYPE_BOOL" => Ok(IndexedValueType::Bool),
        "double" | "INDEXED_VALUE_TYPE_DOUBLE" => Ok(IndexedValueType::Double),
        "datetime" | "INDEXED_VALUE_TYPE_DATETIME" => Ok(IndexedValueType::Datetime),
        "text" | "INDEXED_VALUE_TYPE_TEXT" => Ok(IndexedValueType::Text),
        other => Err(Status::internal(format!(
            "unknown search attribute type in edge response: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{
        interceptors::EdgeInterceptors,
        namespace_cache::{InMemoryNamespaceCache, NamespaceCache},
        operator_service::{InMemoryOperatorApi, OperatorApi},
    };

    #[tokio::test]
    async fn add_search_attributes_upserts_multiple_attributes() {
        let namespaces = Arc::new(InMemoryNamespaceCache::new());
        namespaces
            .insert(crate::namespace_cache::ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
        let service = OperatorService::new(
            api.clone(),
            Arc::new(EdgeInterceptors::permissive(namespaces)),
        );
        let grpc = OperatorServiceGrpc::new(service);

        grpc.add_search_attributes(Request::new(operatorservice::AddSearchAttributesRequest {
            namespace: "default".to_string(),
            search_attributes: [
                (
                    "CustomKeyword".to_string(),
                    IndexedValueType::Keyword as i32,
                ),
                ("CustomInt".to_string(), IndexedValueType::Int as i32),
            ]
            .into_iter()
            .collect(),
        }))
        .await
        .expect("add search attributes should succeed");

        let attrs = api
            .list_search_attributes(Some("default"))
            .await
            .expect("list search attributes should succeed");

        assert_eq!(attrs.len(), 2);
        assert!(
            attrs
                .iter()
                .any(|attr| attr.name == "CustomKeyword" && attr.attr_type == "keyword")
        );
        assert!(
            attrs
                .iter()
                .any(|attr| attr.name == "CustomInt" && attr.attr_type == "int")
        );
    }

    #[tokio::test]
    async fn list_search_attributes_exposes_standard_system_catalog() {
        let namespaces = Arc::new(InMemoryNamespaceCache::new());
        namespaces
            .insert(crate::namespace_cache::ResolvedNamespace::active("default"))
            .await
            .unwrap();
        let api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
        let service = OperatorService::new(api, Arc::new(EdgeInterceptors::permissive(namespaces)));
        let grpc = OperatorServiceGrpc::new(service);

        let response = grpc
            .list_search_attributes(Request::new(operatorservice::ListSearchAttributesRequest {
                namespace: "default".to_string(),
            }))
            .await
            .expect("list search attributes")
            .into_inner();

        assert_eq!(
            response.system_attributes.get("ExecutionDuration").copied(),
            Some(IndexedValueType::Int as i32)
        );
        assert_eq!(
            response.system_attributes.get("TemporalPauseInfo").copied(),
            Some(IndexedValueType::KeywordList as i32)
        );
        assert!(response.custom_attributes.is_empty());
    }
}
