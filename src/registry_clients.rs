use anyhow::{anyhow, bail, Result};
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

    pub(crate) async fn get_client<'a, 'b: 'a>(
        self: &'b mut Self,
        registry: &String,
    ) -> Result<&'a mut RegistryClient> {
        if !self.clients.contains_key(registry) {
            let credential = match docker_credential::get_credential(registry.as_ref()) {
                Ok(DockerCredential::UsernamePassword(u, p)) => Some((u, p)),
                Ok(DockerCredential::IdentityToken(_)) => bail!("Cannot handle tokens"),
                Err(CredentialRetrievalError::NoCredentialConfigured) => None,
                Err(e) => bail!("Error fetching credential: {}", e),
            };

            let builder = RegistryClient::configure().registry(&registry);
            let builder2 = if registry.starts_with("http://") {
                builder.insecure_registry(true)
            } else {
                builder
            };
            let builder3 = if let Some((u, p)) = &credential {
                builder2.username(Some(u.clone())).password(Some(p.clone()))
            } else {
                builder2
            };
            let builder4 = builder3.build()?;
            let v = if let Some(_) = credential {
                builder4.authenticate(&[]).await?
            } else {
                builder4
            };
            self.clients.insert(registry.to_string(), v);
        }

        self.clients
            .get_mut(registry)
            .ok_or_else(|| anyhow!("Unable to get client"))
    }
}
