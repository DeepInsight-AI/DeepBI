# Security Policy

## Supported Versions

The following table outlines the versions of DeepBI that currently receive security updates:

| Version  | Supported          |
|----------|--------------------|
| 2.0.4    | ✅ Yes (Latest)    |
| < 2.0.0  | ❌ No (Upgrade Required) |

## Reporting a Vulnerability

If you discover a security vulnerability in DeepBI, please **DO NOT** post it publicly.

Instead, report it confidentially by emailing our security team:

📧 **Email:** [security@deepbi.com](mailto:security@deepbi.com)

Please include:
- A **detailed description** of the issue.
- Steps to **reproduce the vulnerability**.
- The **affected versions**.
- Potential **impact and severity**.

We take security issues seriously and aim to **respond within 7 business days**.

## Security Best Practices

To ensure the security of your DeepBI installation, follow these best practices:

1. **Keep Dependencies Updated**
   - Regularly update **DeepBI** and its dependencies (`pip`, `npm`, `docker-compose`).
   - Check for vulnerabilities using:
     ```bash
     pip list --outdated
     npm audit
     ```

2. **Use Secure Configuration**
   - **Avoid using default credentials** (e.g., PostgreSQL, Redis).
   - Set strong passwords for database users.
   - **Restrict external database access** by configuring firewalls.

3. **Environment Variables for Secrets**
   - Store sensitive data like API keys, database credentials, and JWT secrets in **`.env`** files.
   - **Never hardcode secrets** in the codebase.

4. **Enable HTTPS**
   - Use **SSL/TLS** (Let's Encrypt or a trusted CA) to secure web access.
   - Configure **reverse proxies (NGINX, Traefik)** to enforce HTTPS.

5. **Container Security (If using Docker)**
   - Use **non-root** users inside containers.
   - Regularly update container images:
     ```bash
     docker pull deepbi/deepbi:latest
     ```
   - Set up a **read-only filesystem** where possible.

6. **Access Control**
   - Implement **role-based access control (RBAC)**.
   - Limit admin privileges to trusted users.

7. **Monitor Logs for Security Events**
   - Enable **audit logging** and monitor `docker logs` or `systemd logs`.
   - Use centralized logging tools like **ELK Stack, Loki, or Splunk**.

## Responsible Disclosure

We follow responsible disclosure principles. If you report a valid security vulnerability, we will:
1. **Acknowledge** your report within 7 days.
2. **Investigate & confirm** the issue.
3. **Issue a fix** in a reasonable timeframe.
4. **Credit** researchers (if applicable and permitted).

## Security Contacts

- 📧 **Email:** [security@deepbi.com](mailto:security@deepbi.com)
- 🌎 **Website:** [https://www.deepbi.com](https://www.deepbi.com)

---

_Last updated: March 2025_
