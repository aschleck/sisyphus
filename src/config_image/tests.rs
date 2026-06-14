use super::*;
use starlark::{
    environment::Module,
    eval::Evaluator,
    syntax::{AstModule, Dialect},
};

// Test Starlark type constructors
#[test]
fn test_starlark_port_creation() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"Port(name="http", number=8080, protocol="TCP")"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let port = result.downcast_ref::<Port>().unwrap();
    assert_eq!(port.name, "http");
    assert_eq!(port.number, Some(8080));
    assert!(matches!(port.protocol, Protocol::TCP));

    Ok(())
}

#[test]
fn test_starlark_port_default_protocol() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"Port(name="http", number=8080)"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let port = result.downcast_ref::<Port>().unwrap();
    assert!(matches!(port.protocol, Protocol::TCP));

    Ok(())
}

#[test]
fn test_starlark_port_udp_protocol() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"Port(name="dns", number=53, protocol="UDP")"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let port = result.downcast_ref::<Port>().unwrap();
    assert!(matches!(port.protocol, Protocol::UDP));

    Ok(())
}

#[test]
fn test_starlark_file_variable_creation() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"FileVariable(name="config", path="/etc/config.yaml")"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let fv = result.downcast_ref::<FileVariable>().unwrap();
    assert_eq!(fv.name, "config");
    assert_eq!(fv.path, "/etc/config.yaml");

    Ok(())
}

#[test]
fn test_starlark_string_variable_creation() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"StringVariable("my_var")"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let sv = result.downcast_ref::<StringVariable>().unwrap();
    assert_eq!(sv.name, "my_var");

    Ok(())
}

#[test]
fn test_starlark_resources_creation() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"Resources(requests={"cpu": "100m", "memory": "128Mi"}, limits={"cpu": "200m"})"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let resources = result.downcast_ref::<Resources>().unwrap();
    assert_eq!(resources.requests.len(), 2);
    assert_eq!(resources.limits.len(), 1);

    Ok(())
}

#[test]
fn test_starlark_application_creation() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"
resources = Resources(requests={"cpu": "100m"})
Application(
    args=["arg1", "arg2"],
    env={"KEY": "value"},
    resources=resources
)
"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let app = result.downcast_ref::<Application>().unwrap();
    assert_eq!(app.args.len(), 2);
    assert_eq!(app.env.len(), 1);

    Ok(())
}

#[test]
fn test_starlark_application_with_port() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"
port = Port(name="http", number=8080)
Application(args=[port])
"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let app = result.downcast_ref::<Application>().unwrap();
    assert_eq!(app.args.len(), 1);

    Ok(())
}

#[test]
fn test_starlark_application_with_varying_args() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"
Application(args=[{"prod": "value1", "dev": "value2"}])
"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let app = result.downcast_ref::<Application>().unwrap();
    assert_eq!(app.args.len(), 1);
    match &app.args[0] {
        ArgumentValues::Varying(map) => {
            assert_eq!(map.len(), 2);
            assert!(map.contains_key("prod"));
            assert!(map.contains_key("dev"));
        }
        _ => panic!("Expected Varying argument"),
    }

    Ok(())
}

#[test]
fn test_starlark_application_with_none_values_filtered() -> anyhow::Result<()> {
    let module = Module::new();
    let globals = make_starlark_globals();

    let mut eval = Evaluator::new(&module);
    let code = r#"
Application(args=[{"prod": "value1", "dev": None}])
"#;
    let ast = AstModule::parse("test", code.to_string(), &Dialect::Standard)
        .map_err(|e| anyhow!("Parse error: {:?}", e))?;
    let result = eval
        .eval_module(ast, &globals)
        .map_err(|e| anyhow!("Eval error: {:?}", e))?;

    let app = result.downcast_ref::<Application>().unwrap();
    assert_eq!(app.args.len(), 1);
    match &app.args[0] {
        ArgumentValues::Varying(map) => {
            // None values should be filtered out
            assert_eq!(map.len(), 1);
            assert!(map.contains_key("prod"));
            assert!(!map.contains_key("dev"));
        }
        _ => panic!("Expected Varying argument"),
    }

    Ok(())
}

#[test]
fn test_config_image_index_deserialization() -> anyhow::Result<()> {
    let json_str = r#"{
        "binary_digest": "sha256:abcd1234",
        "binary_repository": "myrepo/myimage",
        "config_entrypoint": "config.star"
    }"#;

    let index: ConfigImageIndex = serde_json::from_str(json_str)?;
    assert_eq!(index.binary_digest, "sha256:abcd1234");
    assert_eq!(index.binary_repository, "myrepo/myimage");
    assert_eq!(index.config_entrypoint, "config.star");

    Ok(())
}

fn port_app(ports: &[(&str, Option<u16>)]) -> Application {
    let env = ports
        .iter()
        .map(|(name, number)| {
            (
                format!("PORT_{}", name.to_uppercase()),
                ArgumentValues::Uniform(Argument::Port(Port {
                    name: name.to_string(),
                    number: *number,
                    protocol: Protocol::TCP,
                })),
            )
        })
        .collect();
    Application {
        args: Vec::new(),
        env,
        labels: BTreeMap::new(),
        liveness: None,
        readiness: None,
        resources: Resources::default(),
        startup: None,
    }
}

#[test]
fn test_assign_ports_auto_sorted_from_base() -> anyhow::Result<()> {
    let app = port_app(&[("b", None), ("a", None)]);
    let assigned = assign_ports(&app, "prod")?;
    assert_eq!(assigned.get("a"), Some(&8080));
    assert_eq!(assigned.get("b"), Some(&8081));
    Ok(())
}

#[test]
fn test_assign_ports_skips_explicit() -> anyhow::Result<()> {
    // The explicit 8080 should be kept, and the auto-assigned port should skip past it.
    let app = port_app(&[("http", None), ("pinned", Some(8080))]);
    let assigned = assign_ports(&app, "prod")?;
    assert_eq!(assigned.get("pinned"), Some(&8080));
    assert_eq!(assigned.get("http"), Some(&8081));
    Ok(())
}

#[test]
fn test_assign_ports_duplicate_number_errors() {
    let app = port_app(&[("a", Some(8443)), ("b", Some(8443))]);
    let err = assign_ports(&app, "prod").unwrap_err().to_string();
    assert!(err.contains("both request number"), "{}", err);
}

#[test]
fn test_assign_ports_conflicting_number_for_name_errors() {
    let app = Application {
        args: vec![ArgumentValues::Uniform(Argument::Port(Port {
            name: "a".to_string(),
            number: Some(8444),
            protocol: Protocol::TCP,
        }))],
        env: BTreeMap::from([(
            "PORT_A".to_string(),
            ArgumentValues::Uniform(Argument::Port(Port {
                name: "a".to_string(),
                number: Some(8443),
                protocol: Protocol::TCP,
            })),
        )]),
        labels: BTreeMap::new(),
        liveness: None,
        readiness: None,
        resources: Resources::default(),
        startup: None,
    };
    let err = assign_ports(&app, "prod").unwrap_err().to_string();
    assert!(err.contains("conflicting numbers"), "{}", err);
}
