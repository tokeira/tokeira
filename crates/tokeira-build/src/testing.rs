use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{BuildError, ContainerRef, DaggerClient, DirectoryRef, FileRef, SecretRef};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MockCall {
    HostDirectory(String),
    ContainerFrom(String),
    SetSecret(String),
    WithExec(Vec<String>),
    WithEnv { key: String, value: String },
    WithWorkdir(String),
    WithDirectory(String),
    WithFile(String),
    WithEntrypoint(Vec<String>),
    WithUser(String),
    WithRegistryAuth { registry: String, username: String },
    File(String),
    ExportImage(String),
    Publish(String),
}

#[derive(Debug, Default, Clone)]
pub(crate) struct MockDaggerClient {
    state: Arc<Mutex<MockState>>,
}

#[derive(Debug, Default)]
struct MockState {
    calls: Vec<MockCall>,
    fail_publish: bool,
}

impl MockDaggerClient {
    pub(crate) fn calls(&self) -> Vec<MockCall> {
        self.state.lock().expect("mock state lock").calls.clone()
    }

    pub(crate) fn with_publish_error(self) -> Self {
        self.state.lock().expect("mock state lock").fail_publish = true;
        self
    }

    fn record(&self, call: MockCall) {
        self.state.lock().expect("mock state lock").calls.push(call);
    }
}

impl DaggerClient for MockDaggerClient {
    fn host_directory<'client>(
        &'client self,
        path: &Path,
    ) -> Result<Box<dyn DirectoryRef<'client> + 'client>, BuildError> {
        self.record(MockCall::HostDirectory(path.display().to_string()));
        Ok(Box::new(MockDirectory {
            state: Arc::clone(&self.state),
        }))
    }

    fn container_from<'client>(
        &'client self,
        image: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::ContainerFrom(image.to_owned()));
        Ok(Box::new(MockContainer {
            state: Arc::clone(&self.state),
        }))
    }

    fn set_secret<'client>(
        &'client self,
        name: &str,
        _value: &str,
    ) -> Result<Box<dyn SecretRef + 'client>, BuildError> {
        self.record(MockCall::SetSecret(name.to_owned()));
        Ok(Box::new(MockSecret))
    }
}

#[derive(Debug, Clone)]
struct MockContainer {
    state: Arc<Mutex<MockState>>,
}

impl MockContainer {
    fn record(&self, call: MockCall) {
        self.state.lock().expect("mock state lock").calls.push(call);
    }
}

impl<'client> ContainerRef<'client> for MockContainer {
    fn clone_ref(&self) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        Ok(Box::new(self.clone()))
    }

    fn with_exec(
        self: Box<Self>,
        args: &[&str],
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithExec(to_strings(args)));
        Ok(self)
    }

    fn with_env(
        self: Box<Self>,
        key: &str,
        value: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithEnv {
            key: key.to_owned(),
            value: value.to_owned(),
        });
        Ok(self)
    }

    fn with_workdir(
        self: Box<Self>,
        path: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithWorkdir(path.to_owned()));
        Ok(self)
    }

    fn with_directory(
        self: Box<Self>,
        path: &str,
        _dir: &dyn DirectoryRef<'client>,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithDirectory(path.to_owned()));
        Ok(self)
    }

    fn with_file(
        self: Box<Self>,
        path: &str,
        _file: &dyn FileRef<'client>,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithFile(path.to_owned()));
        Ok(self)
    }

    fn with_entrypoint(
        self: Box<Self>,
        args: &[&str],
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithEntrypoint(to_strings(args)));
        Ok(self)
    }

    fn with_user(
        self: Box<Self>,
        user: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithUser(user.to_owned()));
        Ok(self)
    }

    fn with_registry_auth(
        self: Box<Self>,
        registry: &str,
        user: &str,
        _secret: &dyn SecretRef,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError> {
        self.record(MockCall::WithRegistryAuth {
            registry: registry.to_owned(),
            username: user.to_owned(),
        });
        Ok(self)
    }

    fn file(&self, path: &str) -> Result<Box<dyn FileRef<'client> + 'client>, BuildError> {
        self.record(MockCall::File(path.to_owned()));
        Ok(Box::new(MockFile))
    }

    fn export_image(&self, tag: &str) -> Result<(), BuildError> {
        self.record(MockCall::ExportImage(tag.to_owned()));
        Ok(())
    }

    fn publish(&self, remote_ref: &str) -> Result<String, BuildError> {
        self.record(MockCall::Publish(remote_ref.to_owned()));
        if self.state.lock().expect("mock state lock").fail_publish {
            return Err(BuildError::Validation {
                reason: "mock publish failure".to_owned(),
            });
        }
        Ok(format!("{remote_ref}@sha256:mock"))
    }
}

#[derive(Debug, Clone)]
struct MockDirectory {
    state: Arc<Mutex<MockState>>,
}

impl<'client> DirectoryRef<'client> for MockDirectory {
    fn file(&self, name: &str) -> Result<Box<dyn FileRef<'client> + 'client>, BuildError> {
        self.state
            .lock()
            .expect("mock state lock")
            .calls
            .push(MockCall::File(name.to_owned()));
        Ok(Box::new(MockFile))
    }
}

#[derive(Debug, Clone)]
struct MockFile;

impl<'client> FileRef<'client> for MockFile {}

#[derive(Debug, Clone)]
struct MockSecret;

impl SecretRef for MockSecret {}

fn to_strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|arg| (*arg).to_owned()).collect()
}
