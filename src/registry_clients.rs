use anyhow::{anyhow, bail, Result};
use docker_credential::{self, CredentialRetrievalError, DockerCredential};
use docker_registry::{reference::Reference as RegistryReference, v2::Client as RegistryClient};
use std::collections::HashMap;
use std::str::FromStr;

pub(crate) struct RegistryClients {
    clients: HashMap<String, RegistryClient>,
}

impl RegistryClients {
    pub(crate) fn new() -> Self {
        return RegistryClients {
            clients: HashMap::new(),
        };
    }

    pub(crate) async fn get_reference_and_registry<'a, 'b: 'a>(
        self: &'b mut Self,
        registry: &String,
    ) -> Result<(RegistryReference, &'a mut RegistryClient)> {
        let (secure, schemaless) = if registry.starts_with("http://") {
            (false, registry.strip_prefix("http://").unwrap())
        } else if registry.starts_with("https://") {
            (true, registry.strip_prefix("https://").unwrap())
        } else {
            (true, registry.as_str())
        };

        let reference = RegistryReference::from_str(schemaless)
            .map_err(|e| anyhow!("Unable to parse image url: {}", e))?;
        let registry = self.get_client(&reference.registry(), secure).await?;
        Ok((reference, registry))
    }

    async fn get_client<'a, 'b: 'a>(
        self: &'b mut Self,
        registry: &String,
        secure: bool,
    ) -> Result<&'a mut RegistryClient> {
        if !self.clients.contains_key(registry) {
            let credential = match docker_credential::get_credential(registry.as_ref()) {
                Ok(DockerCredential::UsernamePassword(u, p)) => Some((u, p)),
                Ok(DockerCredential::IdentityToken(_)) => bail!("Cannot handle tokens"),
                Err(CredentialRetrievalError::NoCredentialConfigured) => None,
                Err(e) => bail!("Error fetching credential: {}", e),
            };

            let builder = RegistryClient::configure().registry(&registry);
            let builder2 = match secure {
                true => builder,
                false => builder.insecure_registry(true),
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
