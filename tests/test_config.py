import os
from market_validation.config import (
    load_smtp_config,
    load_imap_config,
    load_ollama_config,
    get_smtp_config,
    get_imap_config,
    get_ollama_config,
)


def test_smtp_config_loads_from_env(monkeypatch) -> None:
    monkeypatch.setenv("SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setenv("SMTP_PORT", "587")
    monkeypatch.setenv("SMTP_USER", "test@gmail.com")
    monkeypatch.setenv("SMTP_PASSWORD", "secret")
    monkeypatch.setenv("SMTP_FROM", "noreply@gmail.com")
    
    config = load_smtp_config()
    
    assert config.host == "smtp.gmail.com"
    assert config.port == 587
    assert config.user == "test@gmail.com"
    assert config.password == "secret"
    assert config.from_address == "noreply@gmail.com"


def test_imap_config_falls_back_to_smtp(monkeypatch) -> None:
    monkeypatch.setenv("SMTP_HOST", "mail.example.com")
    monkeypatch.setenv("SMTP_PORT", "993")
    monkeypatch.setenv("SMTP_USER", "user@example.com")
    monkeypatch.setenv("SMTP_PASSWORD", "pass123")
    monkeypatch.delenv("IMAP_HOST", raising=False)
    monkeypatch.delenv("IMAP_PORT", raising=False)
    monkeypatch.delenv("IMAP_USER", raising=False)
    monkeypatch.delenv("IMAP_PASSWORD", raising=False)
    
    config = load_imap_config()
    
    assert config.host == "mail.example.com"
    assert config.port == 993
    assert config.user == "user@example.com"
    assert config.password == "pass123"


def test_imap_config_overrides_smtp(monkeypatch) -> None:
    monkeypatch.setenv("SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("IMAP_HOST", "imap.example.com")
    monkeypatch.setenv("IMAP_PORT", "993")
    monkeypatch.setenv("IMAP_USER", "imap_user@example.com")
    monkeypatch.setenv("IMAP_PASSWORD", "imap_pass")
    
    config = load_imap_config()
    
    assert config.host == "imap.example.com"
    assert config.port == 993
    assert config.user == "imap_user@example.com"
    assert config.password == "imap_pass"


def test_ollama_config_loads_from_env(monkeypatch) -> None:
    monkeypatch.setenv("OLLAMA_API_BASE", "http://remote.ollama.ai:11434")
    monkeypatch.setenv("OLLAMA_MODEL", "llama2:13b")
    
    config = load_ollama_config()
    
    assert config.api_base == "http://remote.ollama.ai:11434"
    assert config.model == "llama2:13b"


def test_ollama_config_defaults(monkeypatch) -> None:
    monkeypatch.delenv("OLLAMA_API_BASE", raising=False)
    monkeypatch.delenv("OLLAMA_MODEL", raising=False)
    
    config = load_ollama_config()
    
    assert config.api_base == "http://localhost:11434"
    assert config.model == "gpt-oss:120b"


def test_get_smtp_config_caches(monkeypatch) -> None:
    monkeypatch.setenv("SMTP_HOST", "test.example.com")
    
    config1 = get_smtp_config()
    config2 = get_smtp_config()
    
    assert config1 is config2


def test_get_imap_config_caches(monkeypatch) -> None:
    monkeypatch.setenv("IMAP_HOST", "test.example.com")
    
    config1 = get_imap_config()
    config2 = get_imap_config()
    
    assert config1 is config2


def test_get_ollama_config_caches(monkeypatch) -> None:
    monkeypatch.setenv("OLLAMA_API_BASE", "http://test.example.com:11434")
    
    config1 = get_ollama_config()
    config2 = get_ollama_config()
    
    assert config1 is config2
