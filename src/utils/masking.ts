/**
 * Utility functions for masking sensitive data in responses.
 * Only used for output - actual connections use real values.
 */

/**
 * Masks sensitive host URLs to prevent exposure of infrastructure details.
 * Examples:
 *   - "pgbouncer-xxx.elb.us-east-1.amazonaws.com" -> "***.elb.us-east-1.amazonaws.com"
 *   - "my-db.xxx.rds.amazonaws.com" -> "***.rds.amazonaws.com"
 *   - "192.168.1.100" -> "***.***.***.***"
 *   - "localhost" -> "localhost" (unchanged)
 */
export function maskHost(host: string | undefined | null): string {
  if (!host) return host as string;

  // Don't mask localhost or simple hostnames
  if (host === 'localhost' || host === '127.0.0.1' || !host.includes('.')) {
    return host;
  }

  // Mask AWS RDS/ELB endpoints
  if (host.includes('.amazonaws.com')) {
    const parts = host.split('.');
    const awsIndex = parts.findIndex(p => p === 'amazonaws');
    if (awsIndex > 0) {
      const suffix = parts.slice(awsIndex - 1).join('.');
      return `***.${suffix}`;
    }
    return '***.amazonaws.com';
  }

  // Mask Azure endpoints
  if (host.includes('.database.windows.net')) return '***.database.windows.net';
  if (host.includes('.azure.com')) return '***.azure.com';

  // Mask GCP endpoints
  if (host.includes('.cloudsql.') || host.includes('.googleapis.com')) return '***.googleapis.com';

  // Mask IP addresses
  if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host)) return '***.***.***.***';

  // For other hosts, mask subdomain but keep domain
  const parts = host.split('.');
  if (parts.length >= 2) return `***.${parts.slice(-2).join('.')}`;

  return host;
}

/**
 * Masks sensitive data in response objects before sending to client.
 */
export function maskResponseData(data: any): any {
  if (!data || typeof data !== 'object') return data;

  // Deep clone to avoid mutating original
  const masked = JSON.parse(JSON.stringify(data));

  // Mask host in server listings
  if (masked.servers && Array.isArray(masked.servers)) {
    masked.servers = masked.servers.map((s: any) => ({
      ...s,
      host: maskHost(s.host)
    }));
  }

  // Mask host in connection info
  if (masked.host) {
    masked.host = maskHost(masked.host);
  }

  return masked;
}
