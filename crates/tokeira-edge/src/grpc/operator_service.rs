use tonic::{Request, Response, Status};

use tokeira_proto::{
    enums::IndexedValueType,
    operatorservice::{self, operator_service_server::OperatorService as OperatorServiceGrpcApi, operator_service_server::OperatorServiceServer},
};

use crate::{
    grpc::metadata::metadata_to_header_map,
    operator_service::{ClusterInfo, OperatorService, SearchAttributeDefinition},
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
        OperatorServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl OperatorServiceGrpcApi for OperatorServiceGrpc {
    async fn get_cluster_info(
        &self,
        request: Request<operatorservice::GetClusterInfoRequest>,
    ) -> Result<Response<operatorservice::GetClusterInfoResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let cluster = self.inner.cluster_info(&headers).await?;
        Ok(Response::new(cluster_info_to_proto(cluster)))
    }

    async fn add_search_attributes(
        &self,
        request: Request<operatorservice::AddSearchAttributesRequest>,
    ) -> Result<Response<operatorservice::AddSearchAttributesResponse>, Status> {
        let headers = metadata_to_header_map(request.metadata());
        let req = request.into_inner();

        for (name, attr_type) in req.custom_attributes {
            let attr = SearchAttributeDefinition {
                name,
                attr_type: indexed_value_type_to_edge(attr_type)?,
            };
            self.inner
                .upsert_search_attribute(&headers, &req.namespace, attr)
                .await?;
        }

        Ok(Response::new(operatorservice::AddSearchAttributesResponse {}))
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
        let attrs = self.inner.list_search_attributes(&headers, namespace).await?;

        Ok(Response::new(operatorservice::ListSearchAttributesResponse {
            system_attributes: Default::default(),
            custom_attributes: attrs
                .into_iter()
                .map(|attr| Ok((attr.name, indexed_value_type_from_edge(&attr.attr_type)? as i32)))
                .collect::<Result<_, Status>>()?,
        }))
    }
}

fn cluster_info_to_proto(cluster: ClusterInfo) -> operatorservice::GetClusterInfoResponse {
    operatorservice::GetClusterInfoResponse {
        cluster_name: cluster.cluster_name,
        server_version: cluster.version,
        http_api_enabled: false,
    }
}

fn indexed_value_type_to_edge(value: i32) -> Result<String, Status> {
    let value = IndexedValueType::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown IndexedValueType: {value}")))?;

    let name = match value {
        IndexedValueType::Unspecified => {
            return Err(Status::invalid_argument("unspecified IndexedValueType"))
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
        namespace_cache::InMemoryNamespaceCache,
        operator_service::{InMemoryOperatorApi, OperatorApi},
    };

    #[tokio::test]
    async fn cluster_info_maps_expected_fields() {
        let api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
        let service = OperatorService::new(
            api,
            Arc::new(EdgeInterceptors::permissive(Arc::new(InMemoryNamespaceCache::new()))),
        );
        let grpc = OperatorServiceGrpc::new(service);

        let response = grpc
            .get_cluster_info(Request::new(operatorservice::GetClusterInfoRequest {}))
            .await
            .expect("cluster info should succeed")
            .into_inner();

        assert_eq!(response.cluster_name, "tokeira-local");
        assert_eq!(response.server_version, "0.1.0-dev");
        assert!(!response.http_api_enabled);
    }

    #[tokio::test]
    async fn add_search_attributes_upserts_multiple_attributes() {
        let namespaces = Arc::new(InMemoryNamespaceCache::new());
        namespaces
            .insert(crate::namespace_cache::ResolvedNamespace::active("default"))
            .await;
        let api = Arc::new(InMemoryOperatorApi::new("tokeira-local"));
        let service = OperatorService::new(
            api.clone(),
            Arc::new(EdgeInterceptors::permissive(namespaces)),
        );
        let grpc = OperatorServiceGrpc::new(service);

        grpc.add_search_attributes(Request::new(operatorservice::AddSearchAttributesRequest {
            namespace: "default".to_string(),
            custom_attributes: [
                ("CustomKeyword".to_string(), IndexedValueType::Keyword as i32),
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
        assert!(attrs.iter().any(|attr| attr.name == "CustomKeyword" && attr.attr_type == "keyword"));
        assert!(attrs.iter().any(|attr| attr.name == "CustomInt" && attr.attr_type == "int"));
    }
}
