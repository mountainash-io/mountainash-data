---
title: Adapters and Auth
description: Credential transformation pipeline including OAuth, JWT, cloud-native, SSL adapters, and auth settings base types
generated_by: claude skill chapter-content-generator
date: 2026-06-03
version: 0.08
---

# Adapters and Auth

## Summary

This chapter explains the adapter pipeline — a composable mechanism for transforming raw credentials into the formats required by specific database drivers. It covers the Credential Transformation concept, then presents concrete adapter implementations for OAuth token exchange, JWT signing, cloud-native IAM authentication, and SSL certificate bundles. The chapter also covers the auth settings base types (NoAuth, PasswordAuth, TokenAuth, IAMAuth) that model different authentication strategies and feed into the adapter pipeline.

## Concepts Covered

- Adapter Pipeline
- Credential Transformation
- OAuth Adapter
- JWT Adapter
- Cloud Native Auth
- SSL Bundle Adapter
- NoAuth Settings
- PasswordAuth Settings
- TokenAuth Settings
- IAMAuth Settings

## Prerequisites

- Chapter 6: Settings and Configuration (ConnectionProfile Base)

---

## The Authentication Challenge

Database connections in modern data engineering environments rarely use simple username/password pairs. Cloud data warehouses require OAuth tokens, JWT-signed credentials, IAM role assumption, or certificate-based mutual TLS. Each database driver expects credentials in a specific format: Snowflake wants an OAuth bearer token as a keyword argument, BigQuery expects a Google credentials object, and Redshift may require an IAM-generated temporary password.

The gap between how credentials are stored (in secrets managers, environment variables, or configuration files) and how drivers consume them requires a transformation layer. mountainash-data's adapter system fills this gap by providing composable transformations that convert stored credentials into driver-ready formats.

## Adapter Pipeline

The **adapter pipeline** is a composable chain of credential transformations that processes raw authentication inputs into the final format expected by a database driver. Each adapter in the pipeline performs a single, well-defined transformation, and adapters can be composed to handle complex authentication flows.

The pipeline concept follows the "pipes and filters" architectural pattern. Raw credentials enter at one end, pass through zero or more adapter transformations, and emerge as driver-ready connection parameters at the other end. The pipeline is configured per-backend, with simple backends (like SQLite) using no adapters and complex backends (like Snowflake with OAuth) using multiple chained adapters.

The conceptual flow of the adapter pipeline is:

1. **Input**: Raw credentials from settings (passwords, tokens, service account keys, certificate paths).
2. **Transform**: One or more adapters process the credentials (e.g., exchange a refresh token for an access token).
3. **Output**: Driver-ready keyword arguments that can be passed directly to the connection builder.

#### Diagram: Adapter Pipeline Flow
<iframe src="../../sims/adapter-pipeline-flow/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Adapter Pipeline Flow</summary>
Type: workflow
**sim-id:** adapter-pipeline-flow<br/>
**Library:** vis-network<br/>
**Status:** Specified

A horizontal pipeline diagram showing credential flow through multiple adapter stages. The left side shows raw credential inputs (password, refresh_token, service_account_json, certificate_path) as colored source nodes. The center shows adapter nodes (OAuth Adapter, JWT Adapter, SSL Adapter) that transform inputs. The right side shows driver-ready output formats (connection_kwargs dict). Animated particles flow through the pipeline showing data transformation. Users can click different authentication scenarios (password, OAuth, certificate) to highlight the relevant path through the pipeline. Learning objective: Analyze how credentials flow through the adapter pipeline for different auth methods (Bloom: Analyze). Controls: click scenario buttons to highlight paths, hover adapters for transformation details. Colors: SteelBlue for inputs, MediumPurple for adapters, DarkGreen for outputs.
</details>

## Credential Transformation

**Credential transformation** is the core concept underlying the adapter pattern: the act of converting credentials from one representation to another. Each transformation is a pure function (or nearly pure, with side effects limited to network calls for token exchange) that takes an input credential format and produces an output format.

Common credential transformations in the database connectivity space include:

- **Password to connection string**: Embedding username and password into a URI-format connection string.
- **Refresh token to access token**: Calling an OAuth token endpoint to exchange a long-lived refresh token for a short-lived access token.
- **Service account key to signed JWT**: Using a private key to sign a JSON Web Token that the database accepts as proof of identity.
- **IAM role to temporary credentials**: Calling AWS STS or GCP IAM to obtain time-limited database credentials from a role.
- **Certificate path to SSL context**: Loading X.509 certificates from the filesystem and constructing an SSL context object.

Each transformation is encapsulated in its own adapter class or function, ensuring that the logic for each credential type remains isolated and testable.

| Transformation | Input | Output | Side Effects |
|---|---|---|---|
| Password embedding | Username + Password | Connection string | None |
| OAuth token exchange | Refresh token + Client credentials | Access token | Network (token endpoint) |
| JWT signing | Private key + Claims | Signed JWT string | None (cryptographic) |
| IAM role assumption | Role ARN + Session credentials | Temporary username/password | Network (STS/IAM API) |
| SSL context creation | Certificate + Key file paths | SSL context object | Filesystem read |

## OAuth Adapter

The **OAuth adapter** handles the OAuth 2.0 token exchange flow required by databases that use OAuth for authentication. Snowflake, Databricks, and other cloud platforms support OAuth as an alternative to password authentication, providing short-lived tokens that reduce the blast radius of credential compromise.

The OAuth adapter performs the following sequence:

1. Accepts a refresh token (or client credentials) from the settings object.
2. Sends a token request to the configured OAuth token endpoint.
3. Receives an access token (and optionally a new refresh token) in the response.
4. Returns the access token in the format expected by the driver (typically as a `token` keyword argument).

The adapter handles token expiration by checking the token's `expires_in` field and re-acquiring a fresh token when needed. This ensures that long-running applications maintain valid credentials without manual intervention.

```python
# Conceptual OAuth adapter flow
class OAuthAdapter:
    def transform(self, settings) -> dict:
        if settings.OAUTH_TOKEN:
            return {"token": settings.OAUTH_TOKEN.get_secret_value()}

        # Exchange client credentials for access token
        response = request_token(
            client_id=settings.OAUTH_CLIENT_ID,
            client_secret=settings.OAUTH_CLIENT_SECRET.get_secret_value(),
            token_endpoint=settings.TOKEN_ENDPOINT,
        )
        return {"token": response["access_token"]}
```

## JWT Adapter

The **JWT adapter** handles JSON Web Token authentication, where a private key is used to sign claims that the database server verifies. This authentication method is common in Snowflake's key-pair authentication and Google Cloud's service account model.

The JWT signing process involves:

1. Loading the private key from a file path or directly from settings.
2. Constructing a claims payload (including issuer, subject, audience, and expiration).
3. Signing the claims with the private key using an appropriate algorithm (typically RS256).
4. Returning the signed JWT as a string that the driver passes to the server.

The adapter differs from OAuth in that no network call is required for signing. The private key is a local secret that produces a verifiable token without contacting any external service. However, the resulting JWT typically has a short validity window (5-60 minutes), so the adapter must be prepared to generate fresh tokens.

```python
# Conceptual JWT adapter flow
class JWTAdapter:
    def transform(self, settings) -> dict:
        private_key = load_private_key(
            key_data=settings.PRIVATE_KEY,
            key_path=settings.PRIVATE_KEY_PATH,
            passphrase=settings.PRIVATE_KEY_PASSPHRASE,
        )
        token = sign_jwt(
            private_key=private_key,
            issuer=settings.USERNAME,
            subject=settings.USERNAME,
            audience=settings.ACCOUNT,
        )
        return {"private_key": private_key, "jwt_token": token}
```

## Cloud Native Auth

**Cloud native authentication** refers to authentication methods that leverage cloud provider IAM (Identity and Access Management) systems rather than database-specific credentials. In this model, the application authenticates to the cloud provider (AWS, GCP, Azure) and then uses that identity to access databases within the same cloud ecosystem.

The primary cloud-native auth patterns are:

- **AWS IAM**: The application assumes an IAM role and receives temporary credentials (access key, secret key, session token) that Redshift or other AWS services accept.
- **Google ADC (Application Default Credentials)**: The application uses ambient credentials from the environment (service account, workload identity, or user credentials from `gcloud auth`) to authenticate to BigQuery.
- **Azure Managed Identity**: The application receives tokens from Azure's identity platform without managing secrets.

Cloud native auth eliminates the need to store database passwords in configuration files or secrets managers. Instead, the identity is derived from the compute environment itself (an EC2 instance role, a GKE workload identity, or an Azure managed identity).

#### Diagram: Cloud Auth Patterns
<iframe src="../../sims/cloud-auth-patterns/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Cloud Auth Patterns</summary>
Type: infographic
**sim-id:** cloud-auth-patterns<br/>
**Library:** vis-network<br/>
**Status:** Specified

A three-panel infographic showing AWS, GCP, and Azure cloud-native authentication flows. Each panel shows: (1) the compute environment (EC2/ECS/Lambda, GKE/Cloud Run, Azure VM/AKS), (2) the IAM service (STS, IAM API, Azure AD), (3) the database service (Redshift, BigQuery, Azure SQL). Arrows show the credential flow: compute requests temporary credentials from IAM, then uses them to connect to the database. Each panel is interactive: clicking the compute node shows the configuration required, clicking the IAM node shows the token format, clicking the database shows the connection parameters. Learning objective: Evaluate which cloud-native auth pattern applies to each cloud provider (Bloom: Evaluate). Controls: click nodes for details, toggle between cloud providers. Colors: Orange for AWS, SteelBlue for GCP, MediumPurple for Azure.
</details>

## SSL Bundle Adapter

The **SSL bundle adapter** handles the transformation of file paths into SSL context objects for database connections that require encrypted transport with mutual TLS (mTLS) authentication. This adapter is particularly relevant for self-hosted databases in enterprise environments where connections must be encrypted and both client and server must prove their identity.

The adapter performs these transformations:

1. Reads the CA certificate file (the certificate authority that signed the server's certificate).
2. Optionally reads the client certificate and client private key (for mutual TLS).
3. Constructs an SSL context object with the appropriate verification mode.
4. Returns the SSL context (or individual file paths, depending on the driver's expectations).

Some database drivers accept raw file paths and handle SSL internally, while others require a pre-configured `ssl.SSLContext` object. The adapter normalizes these differences, accepting file paths from settings and producing whatever format the target driver expects.

```python
# Conceptual SSL adapter flow
class SSLBundleAdapter:
    def transform(self, settings) -> dict:
        ssl_kwargs = {}
        if settings.SSL_CA_CERT:
            ssl_kwargs["ssl_ca"] = settings.SSL_CA_CERT
        if settings.SSL_CLIENT_CERT:
            ssl_kwargs["ssl_cert"] = settings.SSL_CLIENT_CERT
        if settings.SSL_CLIENT_KEY:
            ssl_kwargs["ssl_key"] = settings.SSL_CLIENT_KEY
        return ssl_kwargs
```

## NoAuth Settings

**NoAuth settings** represent the simplest authentication strategy: no credentials are required. This applies to embedded databases (SQLite, local DuckDB) and development environments where authentication is disabled.

The NoAuth strategy sets `AUTH_METHOD` to `"none"` and bypasses all credential validation. The to_driver_kwargs methods return empty dictionaries for credential-related parameters, and only connection parameters (file path, port) are included.

NoAuth is the default for embedded databases because they rely on filesystem permissions rather than application-level authentication. A SQLite database file is secured by its file permissions, not by a password check in the database engine.

## PasswordAuth Settings

**PasswordAuth settings** represent traditional username/password authentication, which is the most common authentication method for server-based databases (PostgreSQL, MySQL, MSSQL). The settings require both `USERNAME` and `PASSWORD` fields to be populated.

The Pydantic model validator on the base class enforces this requirement conditionally: it only fires when `AUTH_METHOD` equals `"password"` and the settings namespace is not a dummy test instance.

```python
@model_validator(mode='after')
def validate_auth_method_password(self) -> Self:
    precondition = self.AUTH_METHOD == CONST_DB_AUTH_METHOD.PASSWORD
    test = self.USERNAME is not None and self.PASSWORD is not None
    valid = (not precondition) | test
    if not valid:
        raise ValueError("USERNAME and PASSWORD required for password authentication")
    return self
```

Password credentials are embedded into the connection string for CONNECTION_STRING mode backends (e.g., `postgres://user:password@host:port/db`) or passed as separate keyword arguments for KWARGS mode backends.

## TokenAuth Settings

**TokenAuth settings** represent authentication via a bearer token, access token, or API key. This method is common for cloud services (MotherDuck, Databricks) where tokens are issued by an external identity provider and presented to the database service.

Token authentication requires a `TOKEN` field to be populated. The token is typically a long-lived API key or a short-lived access token obtained through an OAuth flow.

```python
@model_validator(mode='after')
def validate_auth_method_token(self) -> Self:
    precondition = self.AUTH_METHOD == CONST_DB_AUTH_METHOD.TOKEN
    test = self.TOKEN is not None
    valid = (not precondition) | test
    if not valid:
        raise ValueError("TOKEN required for token authentication")
    return self
```

The key difference between TokenAuth and PasswordAuth from the adapter pipeline's perspective is that tokens are passed as-is (no transformation needed), while passwords are typically embedded in a connection string or hashed.

## IAMAuth Settings

**IAMAuth settings** represent cloud-native authentication through Identity and Access Management services. Rather than storing a password or static token, IAMAuth uses temporary credentials obtained from the cloud provider's IAM service.

IAM authentication combines the Cloud Native Auth adapter with specific configuration fields for the IAM service. For AWS, this means providing a role ARN and region. For GCP, this means providing a service account email or relying on ambient credentials.

IAMAuth depends on the Cloud Native Auth adapter because it requires a network call to the IAM service to obtain temporary credentials. The settings class stores the configuration needed to identify which IAM identity to assume, and the adapter performs the actual credential exchange at connection time.

The following list summarizes the characteristics of each auth settings type:

- **NoAuth**: No credentials; filesystem or network-level security only.
- **PasswordAuth**: Static username + password; embedded in connection string or kwargs.
- **TokenAuth**: Static or refreshable bearer token; passed as-is to driver.
- **IAMAuth**: Dynamic credentials obtained from cloud IAM at connection time; requires network access to IAM service.

!!! warning "Credential lifecycle management"
    Token-based and IAM-based authentication introduce credential expiration as a concern. Short-lived tokens (typical validity: 1-12 hours) require refresh logic, which the adapter pipeline handles transparently. Applications that maintain long-running connections should implement token refresh callbacks or reconnection logic to handle token expiration gracefully.

#### Diagram: Auth Settings Decision Tree
<iframe src="../../sims/auth-settings-decision-tree/main.html" width="100%" height="500px" scrolling="no"></iframe>
<details markdown="1">
<summary>Auth Settings Decision Tree</summary>
Type: diagram
**sim-id:** auth-settings-decision-tree<br/>
**Library:** vis-network<br/>
**Status:** Specified

An interactive decision tree diagram guiding users to the correct auth settings type for their use case. The root node asks "What type of database?" with branches for Embedded (leads to NoAuth), Server (leads to "Do you have a password?" branch), and Cloud (leads to "What credential type?" branch). Each leaf node shows the appropriate auth settings class and required fields. Users can click through the tree to reach their answer, with each node providing contextual help text. The tree also shows which adapters are invoked for each path. Learning objective: Evaluate which authentication strategy to use for a given scenario (Bloom: Evaluate). Controls: click nodes to navigate decision tree, hover for contextual help. Colors: DarkGreen for NoAuth path, SteelBlue for PasswordAuth, Gold for TokenAuth, MediumPurple for IAMAuth.
</details>

## Key Takeaways

- The **adapter pipeline** is a composable chain that transforms raw credentials into driver-ready connection parameters, following the pipes-and-filters pattern.
- **Credential transformation** is the core operation: converting credentials from stored format to the format expected by each database driver.
- The **OAuth adapter** handles token exchange flows for cloud databases that use OAuth 2.0, managing token acquisition and refresh transparently.
- The **JWT adapter** performs local cryptographic signing for key-pair authentication without requiring network calls.
- **Cloud native auth** eliminates stored secrets by deriving database credentials from the compute environment's IAM identity.
- The **SSL bundle adapter** transforms certificate file paths into the SSL configuration format required by each driver.
- Four auth settings base types (**NoAuth**, **PasswordAuth**, **TokenAuth**, **IAMAuth**) model the spectrum from zero credentials to dynamic cloud IAM, with Pydantic validators ensuring the correct fields are populated for each strategy.
- The adapter system is designed for composability: complex authentication flows combine multiple adapters in sequence.
