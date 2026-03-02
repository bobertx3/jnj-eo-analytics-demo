import { useState, useEffect, useCallback } from 'react';

/**
 * Custom hook for API data fetching with loading/error states.
 */
export function useApi(endpoint, options = {}) {
  const { autoFetch = true, defaultValue = null } = options;
  const [data, setData] = useState(defaultValue);
  const [loading, setLoading] = useState(autoFetch);
  const [error, setError] = useState(null);

  const fetchData = useCallback(async (overrideEndpoint) => {
    const url = overrideEndpoint || endpoint;
    if (!url) return;
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch(url);
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
      }
      const json = await resp.json();
      setData(json);
      return json;
    } catch (err) {
      setError(err.message);
      return null;
    } finally {
      setLoading(false);
    }
  }, [endpoint]);

  useEffect(() => {
    if (autoFetch && endpoint) {
      fetchData();
    }
  }, [autoFetch, endpoint, fetchData]);

  return { data, loading, error, refetch: fetchData };
}

/**
 * POST request hook.
 */
export function useApiPost() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const postData = useCallback(async (endpoint, body = {}) => {
    setLoading(true);
    setError(null);
    try {
      const resp = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
      }
      const json = await resp.json();
      setData(json);
      return json;
    } catch (err) {
      setError(err.message);
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  return { data, loading, error, postData };
}

/**
 * Number formatting utilities.
 */
export function formatNumber(num) {
  if (num === null || num === undefined) return '--';
  const n = Number(num);
  if (isNaN(n)) return num;
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

export function formatCurrency(num) {
  if (num === null || num === undefined) return '--';
  const n = Number(num);
  if (isNaN(n)) return num;
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(0)}K`;
  return `$${n.toFixed(0)}`;
}

export function formatDate(dateStr) {
  if (!dateStr) return '--';
  try {
    const d = new Date(dateStr);
    return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
  } catch {
    return dateStr;
  }
}

export function formatDateTime(dateStr) {
  if (!dateStr) return '--';
  try {
    const d = new Date(dateStr);
    return d.toLocaleDateString('en-US', {
      month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit'
    });
  } catch {
    return dateStr;
  }
}
