"""Configuration loading from environment variables."""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class SMTPConfig:
    """SMTP configuration for outbound email."""
    host: str
    port: int
    user: str
    password: str
    from_address: str


@dataclass
class IMAPConfig:
    """IMAP configuration for inbound reply tracking."""
    host: str
    port: int
    user: str
    password: str


@dataclass
class OllamaConfig:
    """Ollama LLM configuration for scoring and drafting."""
    api_base: str
    model: str


def load_smtp_config() -> SMTPConfig:
    """Load SMTP configuration from environment variables."""
    return SMTPConfig(
        host=os.getenv("SMTP_HOST", "localhost"),
        port=int(os.getenv("SMTP_PORT", "1025")),
        user=os.getenv("SMTP_USER", ""),
        password=os.getenv("SMTP_PASSWORD", ""),
        from_address=os.getenv("SMTP_FROM", "noreply@localhost"),
    )


def load_imap_config() -> IMAPConfig:
    """Load IMAP configuration from environment variables.
    
    Falls back to SMTP_* values if IMAP_* are not set.
    """
    smtp_config = load_smtp_config()
    return IMAPConfig(
        host=os.getenv("IMAP_HOST", smtp_config.host),
        port=int(os.getenv("IMAP_PORT", str(smtp_config.port))),
        user=os.getenv("IMAP_USER", smtp_config.user),
        password=os.getenv("IMAP_PASSWORD", smtp_config.password),
    )


def load_ollama_config() -> OllamaConfig:
    """Load Ollama configuration from environment variables."""
    return OllamaConfig(
        api_base=os.getenv("OLLAMA_API_BASE", "http://localhost:11434"),
        model=os.getenv("OLLAMA_MODEL", "gpt-oss:120b"),
    )


# Singleton-style getters for easy access throughout the app
_smtp_config: SMTPConfig | None = None
_imap_config: IMAPConfig | None = None
_ollama_config: OllamaConfig | None = None


def get_smtp_config() -> SMTPConfig:
    """Get cached SMTP configuration."""
    global _smtp_config
    if _smtp_config is None:
        _smtp_config = load_smtp_config()
    return _smtp_config


def get_imap_config() -> IMAPConfig:
    """Get cached IMAP configuration."""
    global _imap_config
    if _imap_config is None:
        _imap_config = load_imap_config()
    return _imap_config


def get_ollama_config() -> OllamaConfig:
    """Get cached Ollama configuration."""
    global _ollama_config
    if _ollama_config is None:
        _ollama_config = load_ollama_config()
    return _ollama_config
