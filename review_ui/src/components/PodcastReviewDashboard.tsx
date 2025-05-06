import React, { useState } from 'react';

// Define an interface for the expected AgentState structure (can be expanded later)
// Based on usage in the component
interface AgentState {
  leads: Array<{ podcast_id: string; name?: string; description?: string; email?: string } | null>; // Allow null for skipped items
  vetting_results: Array<{ podcast_id: string; quality_tier?: string; composite_score?: number } | null>;
  enriched_profiles?: any; // Define more strictly if needed
  execution_status?: string;
  current_step?: string;
}

const CampaignRunner: React.FC = () => {
  // State variables with types
  const [campaignId, setCampaignId] = useState<string>('');
  const [targetAudience, setTargetAudience] = useState<string>('');
  const [isLoading, setIsLoading] = useState<boolean>(false);
  // Use the AgentState interface or null for results
  const [results, setResults] = useState<AgentState | null>(null); 
  const [error, setError] = useState<string>('');

  // Handle form submission with type for event
  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsLoading(true);
    setResults(null);
    setError('');

    try {
      const response = await fetch('http://127.0.0.1:8000/campaigns/run', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          campaign_id: campaignId,
          target_audience: targetAudience,
          // Include other fields from CampaignConfiguration if needed later
        }),
      });

      // Assuming the backend returns { message: string, final_state?: AgentState, error?: string }
      const data: { message: string, final_state?: AgentState, error?: string } = await response.json(); 

      if (!response.ok) {
        // Handle HTTP errors (4xx, 5xx)
        throw new Error(data.error || (data as any).detail || `HTTP error! status: ${response.status}`); // Use 'as any' for detail if needed
      }

      // Success case
      if (data.final_state) {
        setResults(data.final_state);
        console.log("Workflow Results:", data.final_state); // Log for debugging
      } else if (data.error) {
         setError(`Workflow failed: ${data.error}`);
      } else {
          setError("Workflow finished, but no results data returned.");
      }


    } catch (err: any) { // Catch error as any type
        console.error("Error running campaign:", err);
        setError(err.message || 'Failed to run campaign workflow. Check console.');
    } finally {
      setIsLoading(false);
    }
  };

  // Function to render the results table
  const renderResultsTable = () => {
    // Early return if results or leads are missing/empty
    if (!results || !results.leads || results.leads.length === 0) {
      if (results && results.leads && results.leads.length === 0) {
        return <p>Workflow completed, but no leads were found or processed.</p>;
      }
      return null; 
    }

    // Create a map of vetting results by podcast_id for quick lookup
    const vettingMap = (results.vetting_results || []).reduce<{ [key: string]: { quality_tier?: string; composite_score?: number } }>((acc, vetting) => {
      if (vetting && vetting.podcast_id) { 
        acc[vetting.podcast_id] = vetting;
      }
      return acc;
    }, {});

    return (
      <div>
        <h3>Workflow Results (Final State)</h3>
        <p>Status: {results.execution_status || 'N/A'}</p>
        <p>Current Step: {results.current_step || 'N/A'}</p>
        <h4>Leads & Vetting</h4>
        <table border={1} style={{ borderCollapse: 'collapse', width: '100%' }}>
          <thead>
            <tr>
              <th>Podcast Name</th>
              <th>Description</th>
              <th>Email</th>
              <th>Vetting Tier</th>
              <th>Vetting Score</th>
              <th>ID</th>
            </tr>
          </thead>
          <tbody>
            {results.leads.map((lead) => {
              // Skip rendering if lead or id is missing
              if (!lead || !lead.podcast_id) return null; 
              // Find corresponding vetting result, default to empty object if not found
              const vetting = vettingMap[lead.podcast_id] || {}; 
              return (
                <tr key={lead.podcast_id}>
                  <td>{lead.name || 'N/A'}</td>
                  <td>{lead.description || 'N/A'}</td>
                  <td>{lead.email || 'N/A'}</td>
                  <td>{vetting.quality_tier || 'N/A'}</td>
                  {/* Check for undefined before displaying score */}
                  <td>{vetting.composite_score !== undefined ? vetting.composite_score : 'N/A'}</td>
                  <td>{lead.podcast_id}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
        {/* Optionally display enriched profiles if needed later */}
        {/* <pre>{JSON.stringify(results.enriched_profiles, null, 2)}</pre> */}
      </div>
    );
  };


  // Component rendering
  return (
    <div style={{ margin: '20px', padding: '20px', border: '1px solid #ccc' }}>
      <h2>Run Campaign Workflow (Synchronous Demo)</h2>
      <p>Enter campaign details and click Run. The UI will wait for the backend workflow to complete.</p>
      <form onSubmit={handleSubmit} style={{ marginBottom: '20px' }}>
        <div style={{ marginBottom: '10px' }}>
          <label htmlFor="campaignId" style={{ marginRight: '10px' }}>Campaign ID:</label>
          <input
            type="text"
            id="campaignId"
            value={campaignId}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setCampaignId(e.target.value)}
            required
            style={{ padding: '5px' }}
          />
        </div>
        <div style={{ marginBottom: '10px' }}>
          <label htmlFor="targetAudience" style={{ marginRight: '10px' }}>Target Audience:</label>
          <input
            type="text"
            id="targetAudience"
            value={targetAudience}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setTargetAudience(e.target.value)}
            required
            style={{ padding: '5px' }}
          />
        </div>
        <button type="submit" disabled={isLoading} style={{ padding: '8px 15px' }}>
          {isLoading ? 'Running Workflow...' : 'Run Workflow'}
        </button>
      </form>

      {isLoading && <p>Loading results... This might take a minute or more.</p>}
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}
      {results && renderResultsTable()}
       {/* Add a debug view */}
       {/* {results && <pre><code>{JSON.stringify(results, null, 2)}</code></pre>} */}
    </div>
  );
};

export default CampaignRunner; 