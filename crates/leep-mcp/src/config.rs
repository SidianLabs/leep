use leep_cli::cache::Cache;

/// Everything the server needs, normally read from the environment.
#[derive(Clone, Debug, Default)]
pub struct Config {
    pub server: String,
    pub token: Option<String>,
    pub author: String,
    /// Defaults recorded as `agent.*` on every commit; tool arguments win.
    pub agent_model: Option<String>,
    pub agent_session: Option<String>,
    pub agent_prompt: Option<String>,
    pub agent_tests: Option<String>,
    /// Blob cache shared with the `leep` CLI; `None` disables caching.
    pub cache: Option<Cache>,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let server = var("LEEP_SERVER")
            .ok_or("LEEP_SERVER is not set (for example http://127.0.0.1:7070)")?;
        Ok(Config {
            server,
            token: var("LEEP_TOKEN"),
            author: var("LEEP_AUTHOR")
                .or_else(|| var("USER"))
                .unwrap_or_else(|| "agent".into()),
            agent_model: var("LEEP_AGENT_MODEL"),
            agent_session: var("LEEP_AGENT_SESSION"),
            agent_prompt: var("LEEP_AGENT_PROMPT"),
            agent_tests: var("LEEP_AGENT_TESTS"),
            cache: Cache::from_env().ok(),
        })
    }
}
