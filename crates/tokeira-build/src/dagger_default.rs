use std::path::Path;

use crate::{BuildError, ContainerRef, DaggerClient, DirectoryRef, FileRef, SecretRef};

#[derive(Debug)]
pub struct DefaultDaggerClient {
    client: dagger_client::Client,
}

impl DefaultDaggerClient {
    pub fn from_env() -> Result<Self, BuildError> {
        let client = dagger_client::Client::from_env().map_err(|_| BuildError::DaggerMissing)?;
        Ok(Self { client })
    }
}

impl DaggerClient for DefaultDaggerClient {
    fn host_directory<'client>(
        &'client self,
        path: &Path,
    ) -> Result<Box<dyn DirectoryRef<'client> + 'client>, BuildError> {
        let path = path.to_str().ok_or_else(|| {
            validation_error(format!("path is not valid UTF-8: {}", path.display()))
        })?;
        self.client
            .host_directory(path)
            .map(|dir| Box::new(dir) as Box<dyn DirectoryRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn container_from<'client>(
        &'client self,
        image: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.client
            .container_from(image)
            .map(|container| Box::new(container) as Box<dyn ContainerRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn set_secret<'client>(
        &'client self,
        name: &str,
        value: &str,
    ) -> Result<Box<dyn SecretRef + 'client>, BuildError> {
        self.client
            .set_secret(name, value)
            .map(|secret| Box::new(secret) as Box<dyn SecretRef + 'client>)
            .map_err(dagger_error)
    }
}

impl<'client> ContainerRef<'client> for dagger_client::Container<'client> {
    fn clone_ref(&self) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        Ok(Box::new(self.clone()))
    }

    fn with_exec(
        self: Box<Self>,
        args: &[&str],
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        (*self)
            .with_exec(args)
            .map(|container| Box::new(container) as Box<dyn ContainerRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn with_env(
        self: Box<Self>,
        key: &str,
        value: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        (*self)
            .with_env_variable(key, value)
            .map(|container| Box::new(container) as Box<dyn ContainerRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn with_workdir(
        self: Box<Self>,
        path: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        (*self)
            .with_workdir(path)
            .map(|container| Box::new(container) as Box<dyn ContainerRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn with_directory(
        self: Box<Self>,
        path: &str,
        dir: &dyn DirectoryRef<'client>,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        let dir = dir.as_dagger_directory().ok_or_else(|| {
            validation_error("directory handle was not created by DefaultDaggerClient")
        })?;
        (*self)
            .with_directory(path, dir)
            .map(|container| Box::new(container) as Box<dyn ContainerRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn with_file(
        self: Box<Self>,
        path: &str,
        file: &dyn FileRef<'client>,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        let file = file.as_dagger_file().ok_or_else(|| {
            validation_error("file handle was not created by DefaultDaggerClient")
        })?;
        (*self)
            .with_file(path, file)
            .map(|container| Box::new(container) as Box<dyn ContainerRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn with_entrypoint(
        self: Box<Self>,
        args: &[&str],
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        (*self)
            .with_entrypoint(args)
            .map(|container| Box::new(container) as Box<dyn ContainerRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn with_user(
        self: Box<Self>,
        user: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        (*self)
            .with_user(user)
            .map(|container| Box::new(container) as Box<dyn ContainerRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn with_registry_auth(
        self: Box<Self>,
        registry: &str,
        user: &str,
        secret: &dyn SecretRef,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        let secret = secret.as_dagger_secret().ok_or_else(|| {
            validation_error("secret handle was not created by DefaultDaggerClient")
        })?;
        (*self)
            .with_registry_auth(registry, user, secret)
            .map(|container| Box::new(container) as Box<dyn ContainerRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn file(&self, path: &str) -> Result<Box<dyn FileRef<'client> + 'client>, BuildError> {
        self.file(path)
            .map(|file| Box::new(file) as Box<dyn FileRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn export_image(&self, tag: &str) -> Result<(), BuildError> {
        self.export_image(tag).map_err(dagger_error)
    }

    fn publish(&self, remote_ref: &str) -> Result<String, BuildError> {
        self.publish(remote_ref).map_err(dagger_error)
    }

    fn as_dagger_container(&self) -> Option<&dagger_client::Container<'client>> {
        Some(self)
    }
}

impl<'client> DirectoryRef<'client> for dagger_client::Directory<'client> {
    fn file(&self, name: &str) -> Result<Box<dyn FileRef<'client> + 'client>, BuildError> {
        self.file(name)
            .map(|file| Box::new(file) as Box<dyn FileRef<'client> + 'client>)
            .map_err(dagger_error)
    }

    fn as_dagger_directory(&self) -> Option<&dagger_client::Directory<'client>> {
        Some(self)
    }
}

impl<'client> FileRef<'client> for dagger_client::File<'client> {
    fn as_dagger_file(&self) -> Option<&dagger_client::File<'client>> {
        Some(self)
    }
}

impl SecretRef for dagger_client::SecretId {
    fn as_dagger_secret(&self) -> Option<&dagger_client::SecretId> {
        Some(self)
    }
}

fn dagger_error(source: eyre::Report) -> BuildError {
    BuildError::Validation {
        reason: source.to_string(),
    }
}

fn validation_error(reason: impl Into<String>) -> BuildError {
    BuildError::Validation {
        reason: reason.into(),
    }
}
