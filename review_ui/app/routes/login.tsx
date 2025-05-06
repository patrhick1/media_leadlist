import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router';

export default function LoginPage() {
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    if (localStorage.getItem('isLoggedInPGL') === 'true') {
      navigate('/home', { replace: true });
    }
  }, [navigate]);

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setIsLoading(true);
    setError('');

    try {
      const response = await fetch('/auth/validate-login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ password: password }),
      });

      if (response.ok) {
        localStorage.setItem('isLoggedInPGL', 'true');
        navigate('/home', { replace: true });
      } else {
        const errorData = await response.json().catch(() => ({ detail: 'Invalid password or server error.' }));
        localStorage.removeItem('isLoggedInPGL');
        setError(errorData.detail || 'Invalid password.');
      }
    } catch (err) {
      localStorage.removeItem('isLoggedInPGL');
      setError('Login request failed. Please check your connection and try again.');
      console.error("Login error:", err);
    }
    setIsLoading(false);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', minHeight: '100vh', backgroundColor: '#f0f2f5', fontFamily: 'sans-serif' }}>
      <div style={{ padding: '40px', backgroundColor: 'white', borderRadius: '8px', boxShadow: '0 4px 12px rgba(0,0,0,0.1)', width: '100%', maxWidth: '400px' }}>
        <h1 style={{ fontSize: '24px', textAlign: 'center', marginBottom: '25px', color: '#333' }}>PGL System Login</h1>
        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          <div>
            <label htmlFor="password" style={{ display: 'block', marginBottom: '8px', color: '#555', fontSize: '14px' }}>Password:</label>
            <input
              type="password"
              id="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              disabled={isLoading}
              style={{ width: '100%', padding: '12px', border: '1px solid #ccc', borderRadius: '4px', boxSizing: 'border-box' }}
              placeholder="Enter password"
            />
          </div>
          {error && <p style={{ color: 'red', fontSize: '14px', textAlign: 'center', margin: '0' }}>{error}</p>}
          <button
            type="submit"
            disabled={isLoading}
            style={{
              padding: '12px 15px',
              backgroundColor: isLoading ? '#B0BEC5' : '#007bff',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: isLoading ? 'not-allowed' : 'pointer',
              fontSize: '16px',
              fontWeight: '500'
            }}
          >
            {isLoading ? 'Logging in...' : 'Login'}
          </button>
        </form>
      </div>
    </div>
  );
} 