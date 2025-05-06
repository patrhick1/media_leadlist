import React, { useState, useEffect } from 'react';

// Basic styling
const dashboardStyle = {
    border: '1px solid lightblue',
    borderRadius: '8px',
    padding: '15px',
    margin: '20px auto',
    maxWidth: '900px',
    backgroundColor: '#f0f8ff',
};

const sectionStyle = {
    marginBottom: '15px',
    paddingBottom: '10px',
    borderBottom: '1px dashed lightblue',
};

const errorStyle = {
    color: 'red',
    fontWeight: 'bold',
};

function AnalyticsDashboard() {
  // State for different analytics data sections
  const [stepDurations, setStepDurations] = useState(null);
  const [vettingDist, setVettingDist] = useState(null);
  // Add state for other sections later (search, crm, suggestions)
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

  useEffect(() => {
    const fetchAnalytics = async () => {
      setIsLoading(true);
      setError(null);
      try {
        // Fetch multiple endpoints in parallel
        const [durationsRes, distRes] = await Promise.all([
          fetch(`${API_BASE_URL}/analytics/step-durations`),
          fetch(`${API_BASE_URL}/analytics/vetting-distribution`),
          // Add fetch calls for other endpoints here later
        ]);

        // Process Durations
        if (durationsRes.ok) {
          setStepDurations(await durationsRes.json());
        } else {
          console.error("Failed to fetch step durations:", durationsRes.statusText);
          // Store partial errors if needed
        }

        // Process Distribution
        if (distRes.ok) {
          setVettingDist(await distRes.json());
        } else {
          console.error("Failed to fetch vetting distribution:", distRes.statusText);
        }

        // Basic overall error handling - refine if needed
        if (!durationsRes.ok || !distRes.ok) {
             throw new Error("Failed to load some analytics data.");
        }
        
      } catch (err) {
        console.error('Error fetching analytics:', err);
        setError(err.message);
      } finally {
        setIsLoading(false);
      }
    };

    fetchAnalytics();
  }, [API_BASE_URL]); // Re-fetch if base URL changes

  // Helper to render duration data
  const renderDurations = () => {
      if (!stepDurations) return <p>No duration data available.</p>;
      return (
          <ul>
              {Object.entries(stepDurations).map(([step, data]) => (
                  <li key={step}>
                      <strong>{step}:</strong> 
                      Avg: {data.avg_duration_ms ? (data.avg_duration_ms / 1000).toFixed(1) : 'N/A'}s, 
                      Median: {data.median_duration_ms ? (data.median_duration_ms / 1000).toFixed(1) : 'N/A'}s
                  </li>
              ))}
          </ul>
      );
  };

  // Helper to render distribution data
  const renderDistribution = () => {
      if (!vettingDist) return <p>No vetting distribution data available.</p>;
       const total = Object.values(vettingDist).reduce((sum, count) => sum + count, 0);
      return (
          <ul>
              {Object.entries(vettingDist).map(([tier, count]) => (
                  <li key={tier}>
                      <strong>Tier {tier}:</strong> {count} leads ({total > 0 ? ((count / total) * 100).toFixed(1) : 0}%) 
                  </li>
              ))}
          </ul>
      );
  };

  return (
    <div style={dashboardStyle}>
      <h3>Analytics Overview</h3>
      {isLoading && <p>Loading analytics...</p>}
      {error && <p style={errorStyle}>Error loading analytics: {error}</p>}
      
      {!isLoading && !error && (
          <>
              <div style={sectionStyle}>
                  <h4>Step Durations (Average/Median)</h4>
                  {renderDurations()}
              </div>
              <div style={sectionStyle}>
                  <h4>Vetting Tier Distribution</h4>
                  {renderDistribution()}
              </div>
              {/* Add sections for other analytics here */}
          </>
      )}
    </div>
  );
}

export default AnalyticsDashboard; 