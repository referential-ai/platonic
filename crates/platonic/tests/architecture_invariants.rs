use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

const CORE_DEPENDENCIES: [&str; 3] = ["serde", "serde_json", "thiserror"];
const FORBIDDEN_AGENT_DEPENDENCIES: [&str; 2] = ["platonic-server", "platonic"];
const OWNED_BINARIES: [(&str, &str); 3] = [
    ("platonic", "platonic"),
    ("plato", "plato-agent"),
    ("plato-tui", "plato-agent"),
];
const FORBIDDEN_BINARIES: [&str; 2] = ["plato-agentd", "plato-gateway-discord"];

#[derive(Debug)]
struct Workspace {
    packages: Vec<Package>,
}

#[derive(Debug)]
struct Package {
    name: String,
    dependencies: Vec<Dependency>,
    binaries: Vec<String>,
}

#[derive(Debug)]
struct Dependency {
    name: String,
    normal: bool,
    workspace_package: Option<String>,
}

impl Workspace {
    fn from_metadata_json(json: &[u8]) -> Result<Self, String> {
        let metadata: Value = serde_json::from_slice(json)
            .map_err(|error| format!("cargo metadata returned invalid JSON: {error}"))?;
        let package_values = json_array(&metadata, "packages", "cargo metadata root")?;
        let workspace_members = json_array(&metadata, "workspace_members", "cargo metadata root")?
            .iter()
            .map(|member| {
                member
                    .as_str()
                    .ok_or_else(|| "cargo metadata workspace member ID is not a string".to_owned())
            })
            .collect::<Result<BTreeSet<_>, _>>()?;

        let mut workspace_package_dirs = BTreeMap::new();
        for package in package_values {
            let id = json_string(package, "id", "package")?;
            if !workspace_members.contains(id) {
                continue;
            }
            let name = json_string(package, "name", "workspace package")?;
            let manifest_path = json_string(package, "manifest_path", name)?;
            let package_dir = Path::new(manifest_path).parent().ok_or_else(|| {
                format!("workspace package `{name}` has a manifest path without a parent")
            })?;
            workspace_package_dirs.insert(package_dir.to_path_buf(), name.to_owned());
        }

        let mut packages = Vec::new();
        for package in package_values {
            let id = json_string(package, "id", "package")?;
            if !workspace_members.contains(id) {
                continue;
            }
            let name = json_string(package, "name", "workspace package")?.to_owned();
            let dependencies = json_array(package, "dependencies", &name)?
                .iter()
                .map(|dependency| {
                    let dependency_name = json_string(dependency, "name", &name)?.to_owned();
                    let kind = dependency.get("kind").ok_or_else(|| {
                        format!("dependency `{dependency_name}` of package `{name}` has no kind")
                    })?;
                    let path = optional_json_string(dependency, "path", &dependency_name)?;
                    Ok(Dependency {
                        name: dependency_name,
                        normal: kind.is_null(),
                        workspace_package: path.and_then(|path| {
                            workspace_package_dirs.get(&PathBuf::from(path)).cloned()
                        }),
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let binaries = json_array(package, "targets", &name)?
                .iter()
                .map(|target| {
                    let kinds = json_array(target, "kind", &name)?;
                    let target_name = json_string(target, "name", &name)?;
                    Ok(kinds
                        .iter()
                        .any(|kind| kind.as_str() == Some("bin"))
                        .then(|| target_name.to_owned()))
                })
                .collect::<Result<Vec<_>, String>>()?
                .into_iter()
                .flatten()
                .collect();
            packages.push(Package {
                name,
                dependencies,
                binaries,
            });
        }

        Ok(Self { packages })
    }

    fn package(&self, name: &str) -> Option<&Package> {
        self.packages.iter().find(|package| package.name == name)
    }

    fn dependency_path(&self, root: &str, destination: &str) -> Option<Vec<String>> {
        let mut queue = VecDeque::from([vec![root.to_owned()]]);
        let mut visited = BTreeSet::from([root.to_owned()]);

        while let Some(path) = queue.pop_front() {
            let package = self.package(path.last()?)?;
            let mut dependencies = package
                .dependencies
                .iter()
                .filter_map(|dependency| dependency.workspace_package.as_deref())
                .collect::<Vec<_>>();
            dependencies.sort_unstable();

            for dependency in dependencies {
                let mut dependency_path = path.clone();
                dependency_path.push(dependency.to_owned());
                if dependency == destination {
                    return Some(dependency_path);
                }
                if visited.insert(dependency.to_owned()) {
                    queue.push_back(dependency_path);
                }
            }
        }

        None
    }
}

fn json_array<'a>(value: &'a Value, field: &str, context: &str) -> Result<&'a Vec<Value>, String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{context} has no JSON array field `{field}`"))
}

fn json_string<'a>(value: &'a Value, field: &str, context: &str) -> Result<&'a str, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{context} has no JSON string field `{field}`"))
}

fn optional_json_string<'a>(
    value: &'a Value,
    field: &str,
    context: &str,
) -> Result<Option<&'a str>, String> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(string)) => Ok(Some(string)),
        _ => Err(format!(
            "{context} has neither a JSON string nor null field `{field}`"
        )),
    }
}

fn core_dependency_failures(workspace: &Workspace) -> Vec<String> {
    let Some(core) = workspace.package("platonic-core") else {
        return vec!["required workspace package `platonic-core` is missing".to_owned()];
    };
    let mut failures = core
        .dependencies
        .iter()
        .filter_map(|dependency| {
            dependency.workspace_package.as_ref().map(|package| {
                format!(
                    "package `platonic-core` depends on workspace package `{package}` through dependency `{}`",
                    dependency.name
                )
            })
        })
        .collect::<Vec<_>>();

    let expected = CORE_DEPENDENCIES.into_iter().collect::<BTreeSet<_>>();
    let actual = core
        .dependencies
        .iter()
        .filter(|dependency| dependency.normal)
        .map(|dependency| dependency.name.as_str())
        .collect::<BTreeSet<_>>();
    if actual != expected {
        let missing = expected
            .difference(&actual)
            .map(|dependency| (*dependency).to_owned())
            .collect::<Vec<_>>();
        let unexpected = actual
            .difference(&expected)
            .map(|dependency| (*dependency).to_owned())
            .collect::<Vec<_>>();
        failures.push(format!(
            "package `platonic-core` normal direct dependencies must be exactly `serde`, `serde_json`, and `thiserror`; missing: {}; unexpected: {}",
            display_names(&missing),
            display_names(&unexpected)
        ));
    }

    failures
}

fn agent_dependency_failures(workspace: &Workspace) -> Vec<String> {
    if workspace.package("plato-agent").is_none() {
        return vec!["required workspace package `plato-agent` is missing".to_owned()];
    }

    FORBIDDEN_AGENT_DEPENDENCIES
        .into_iter()
        .filter_map(|forbidden| {
            workspace
                .dependency_path("plato-agent", forbidden)
                .map(|path| {
                    format!(
                        "workspace dependency closure rooted at `plato-agent` reaches forbidden package `{forbidden}` via {}",
                        path.join(" -> ")
                    )
                })
        })
        .collect()
}

fn binary_ownership_failures(workspace: &Workspace) -> Vec<String> {
    let mut owners = BTreeMap::<&str, BTreeSet<&str>>::new();
    for package in &workspace.packages {
        for binary in &package.binaries {
            owners
                .entry(binary)
                .or_default()
                .insert(package.name.as_str());
        }
    }

    let mut failures = Vec::new();
    for (binary, expected_owner) in OWNED_BINARIES {
        let actual_owners = owners.get(binary).cloned().unwrap_or_default();
        if actual_owners != BTreeSet::from([expected_owner]) {
            let actual_owners = actual_owners
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            failures.push(format!(
                "binary `{binary}` must be provided only by package `{expected_owner}`; found owners: {}",
                display_names(&actual_owners)
            ));
        }
    }
    for binary in FORBIDDEN_BINARIES {
        if let Some(actual_owners) = owners.get(binary) {
            for owner in actual_owners {
                failures.push(format!(
                    "package `{owner}` provides forbidden binary `{binary}`"
                ));
            }
        }
    }

    failures
}

fn display_names(names: &[String]) -> String {
    if names.is_empty() {
        "none".to_owned()
    } else {
        names
            .iter()
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn workspace_architecture_failures(workspace: &Workspace) -> Vec<String> {
    let mut failures = core_dependency_failures(workspace);
    failures.extend(agent_dependency_failures(workspace));
    failures.extend(binary_ownership_failures(workspace));
    failures
}

fn workspace_metadata() -> Workspace {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args(["metadata", "--locked", "--no-deps", "--format-version", "1"])
        .current_dir(workspace_root)
        .output()
        .expect("failed to run `cargo metadata --locked --no-deps`");
    assert!(
        output.status.success(),
        "`cargo metadata --locked --no-deps` failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    Workspace::from_metadata_json(&output.stdout)
        .unwrap_or_else(|error| panic!("failed to read cargo metadata: {error}"))
}

#[test]
fn workspace_architecture_invariants_hold() {
    let failures = workspace_architecture_failures(&workspace_metadata());
    assert!(
        failures.is_empty(),
        "architecture invariant violations:\n- {}",
        failures.join("\n- ")
    );
}

#[test]
fn architecture_invariant_failures_are_actionable() {
    let core_fixture = Workspace {
        packages: vec![Package {
            name: "platonic-core".to_owned(),
            dependencies: vec![
                external_dependency("serde"),
                external_dependency("serde_json"),
                external_dependency("tokio"),
                workspace_dependency("platonic-protocol"),
            ],
            binaries: vec![],
        }],
    };
    assert_eq!(
        core_dependency_failures(&core_fixture),
        [
            "package `platonic-core` depends on workspace package `platonic-protocol` through dependency `platonic-protocol`",
            "package `platonic-core` normal direct dependencies must be exactly `serde`, `serde_json`, and `thiserror`; missing: `thiserror`; unexpected: `platonic-protocol`, `tokio`",
        ]
    );

    let agent_fixture = Workspace {
        packages: vec![
            fixture_package("plato-agent", &["platonic-client", "platonic"], &[]),
            fixture_package("platonic-client", &["platonic-server"], &[]),
            fixture_package("platonic-server", &[], &[]),
            fixture_package("platonic", &[], &[]),
        ],
    };
    assert_eq!(
        agent_dependency_failures(&agent_fixture),
        [
            "workspace dependency closure rooted at `plato-agent` reaches forbidden package `platonic-server` via plato-agent -> platonic-client -> platonic-server",
            "workspace dependency closure rooted at `plato-agent` reaches forbidden package `platonic` via plato-agent -> platonic",
        ]
    );

    let binary_fixture = Workspace {
        packages: vec![
            fixture_package("platonic", &[], &["platonic"]),
            fixture_package("plato-agent", &[], &["plato"]),
            fixture_package(
                "wrong-package",
                &[],
                &["platonic", "plato", "plato-agentd", "plato-gateway-discord"],
            ),
        ],
    };
    assert_eq!(
        binary_ownership_failures(&binary_fixture),
        [
            "binary `platonic` must be provided only by package `platonic`; found owners: `platonic`, `wrong-package`",
            "binary `plato` must be provided only by package `plato-agent`; found owners: `plato-agent`, `wrong-package`",
            "binary `plato-tui` must be provided only by package `plato-agent`; found owners: none",
            "package `wrong-package` provides forbidden binary `plato-agentd`",
            "package `wrong-package` provides forbidden binary `plato-gateway-discord`",
        ]
    );
}

fn external_dependency(name: &str) -> Dependency {
    Dependency {
        name: name.to_owned(),
        normal: true,
        workspace_package: None,
    }
}

fn workspace_dependency(name: &str) -> Dependency {
    Dependency {
        name: name.to_owned(),
        normal: true,
        workspace_package: Some(name.to_owned()),
    }
}

fn fixture_package(name: &str, dependencies: &[&str], binaries: &[&str]) -> Package {
    Package {
        name: name.to_owned(),
        dependencies: dependencies
            .iter()
            .map(|dependency| workspace_dependency(dependency))
            .collect(),
        binaries: binaries.iter().map(|binary| (*binary).to_owned()).collect(),
    }
}
