use anyhow::{Result, anyhow, bail};
use docker_credential::{self, CredentialRetrievalError, DockerCredential};
use docker_registry::v2::Client as RegistryClient;
use std::collections::HashMap;

pub(crate) struct RegistryClients {
    clients: HashMap<String, RegistryClient>,
}

impl RegistryClients {
    pub(crate) fn new() -> Self {
        return RegistryClients {
            clients: HashMap::new(),
        };
    }

    pub(crate) fn get_client<'a, 'b: 'a>(
        self: &'b mut Self,
        registry: impl AsRef<str>,
    ) -> Result<&'a mut RegistryClient> {
        if !self.clients.contains_key(registry.as_ref()) {
            let credential = match docker_credential::get_credential(registry.as_ref()) {
                Ok(DockerCredential::UsernamePassword(u, p)) => Some((u, p)),
                Ok(DockerCredential::IdentityToken(_)) => bail!("Cannot handle tokens"),
                Err(CredentialRetrievalError::NoCredentialConfigured) => None,
                Err(e) => bail!("Error fetching credential: {}", e),
            };

            let builder = RegistryClient::configure()
                .registry(registry.as_ref())
                .insecure_registry(true);
            let builder2 = if let Some((u, p)) = credential {
                builder.username(Some(u)).password(Some(p))
            } else {
                builder
            };
            let v = builder2.build()?;
            self.clients.insert(registry.as_ref().to_string(), v);
        }

        self.clients
            .get_mut(registry.as_ref())
            .ok_or_else(|| anyhow!("Unable to get client"))
    }
}
