import React, { useState } from 'react';
import PropTypes from 'prop-types';

// Basic styling for the card (inline for simplicity, consider CSS Modules or Tailwind later)
const cardStyleBase = {
  border: '1px solid #ddd',
  borderRadius: '8px',
  padding: '16px',
  marginBottom: '16px',
  boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
  backgroundColor: '#fff',
  position: 'relative', // Needed for absolute positioning of checkbox
  transition: 'box-shadow 0.2s ease-in-out, border-color 0.2s ease-in-out', // Add transition
};

const cardStyleActive = {
    ...cardStyleBase,
    borderColor: 'blue', // Highlight border when active
    boxShadow: '0 4px 8px rgba(0, 0, 255, 0.2)', // Add a subtle blue shadow
};

const checkboxStyle = {
    position: 'absolute',
    top: '10px',
    left: '10px',
    transform: 'scale(1.2)', // Make checkbox slightly larger
    cursor: 'pointer',
};

const headerStyle = {
  fontSize: '1.2em',
  fontWeight: 'bold',
  marginBottom: '8px',
  marginLeft: '30px', // Add margin to avoid overlapping with checkbox
};

const descriptionStyle = {
  fontSize: '0.9em',
  color: '#555',
  marginBottom: '12px',
  marginLeft: '30px', // Add margin
};

const emailStyle = {
    fontSize: '0.8em',
    color: '#777',
    marginBottom: '8px',
    marginLeft: '30px', // Add margin
};

const vettingInfoStyle = {
  borderTop: '1px solid #eee',
  paddingTop: '12px',
  marginTop: '12px',
  fontSize: '0.9em',
};

const tierStyle = (tier) => ({
  fontWeight: 'bold',
  color: tier === 'A' ? 'green' : tier === 'B' ? 'orange' : 'red',
  marginRight: '8px',
});

const feedbackTextAreaStyle = {
  width: '100%', // Take full width
  minHeight: '60px',
  marginTop: '10px',
  padding: '8px',
  border: '1px solid #ccc',
  borderRadius: '4px',
  boxSizing: 'border-box', // Include padding and border in the element's total width and height
  fontSize: '0.9em',
};

// Accept onApprove, onReject, and selection props
function LeadCard({ lead, onApprove, onReject, isSelected, onSelectChange, isActive }) {
  // State for feedback text
  const [feedbackText, setFeedbackText] = useState('');

  // Destructure with default values for safety
  const { lead_info = {}, vetting_info = {} } = lead || {};
  // Ensure we have podcast_id
  const { podcast_id, name = 'N/A', description = 'No description available.', email } = lead_info;
  const { quality_tier = 'N/A', explanation = 'No vetting explanation.', composite_score } = vetting_info;

  // Button click handlers
  const handleApproveClick = () => {
    if (podcast_id && onApprove) {
      onApprove(podcast_id, feedbackText || null); // Pass feedback (or null if empty)
    }
  };

  const handleRejectClick = () => {
    if (podcast_id && onReject) {
      onReject(podcast_id, feedbackText || null); // Pass feedback (or null if empty)
    }
  };

  const handleFeedbackChange = (event) => {
    setFeedbackText(event.target.value);
  };

  // --- NEW: Checkbox change handler --- 
  const handleCheckboxChange = (event) => {
      if (podcast_id && onSelectChange) {
          onSelectChange(podcast_id, event.target.checked);
      }
  };

  // Basic check if podcast_id exists to enable buttons
  const buttonsDisabled = !podcast_id;

  // Determine card style based on active state
  const currentCardStyle = isActive ? cardStyleActive : cardStyleBase;

  return (
    <div style={currentCardStyle}>
      {/* --- NEW: Checkbox --- */} 
      <input 
        type="checkbox" 
        style={checkboxStyle}
        checked={isSelected} 
        onChange={handleCheckboxChange}
        disabled={!podcast_id} // Disable if no ID
        aria-label={`Select lead ${name}`} // Accessibility
      />
      
      {/* Adjust content position due to checkbox */} 
      <div style={headerStyle}>{name}</div>
      {email && <div style={emailStyle}>{email}</div>}
      <div style={descriptionStyle}>{description}</div>

      <div style={vettingInfoStyle}>
        <strong>Vetting Info:</strong>
        <div>
          <span style={tierStyle(quality_tier)}>Tier: {quality_tier}</span>
          {composite_score !== undefined && <span>(Score: {composite_score})</span>}
        </div>
        <div>
          <i>Explanation: {explanation}</i>
        </div>
      </div>

      {/* Feedback Text Area */}
      <div style={{ marginTop: '15px' }}>
        <label htmlFor={`feedback-${podcast_id}`} style={{ display: 'block', marginBottom: '5px', fontSize: '0.9em', fontWeight: 'bold' }}>
          Optional Feedback:
        </label>
        <textarea
          id={`feedback-${podcast_id}`} // Unique ID for label association
          style={feedbackTextAreaStyle}
          value={feedbackText}
          onChange={handleFeedbackChange}
          placeholder="Provide feedback (optional)..."
          rows={3}
          disabled={buttonsDisabled}
        />
      </div>

      {/* Functional buttons */}
      <div style={{ marginTop: '10px', textAlign: 'right' }}>
        <button onClick={handleRejectClick} disabled={buttonsDisabled} style={{ marginRight: '8px' }}>
          Reject
        </button>
        <button onClick={handleApproveClick} disabled={buttonsDisabled}>
          Approve
        </button>
      </div>
    </div>
  );
}

LeadCard.propTypes = {
  lead: PropTypes.shape({
    lead_info: PropTypes.shape({
      podcast_id: PropTypes.string.isRequired,
      name: PropTypes.string,
      description: PropTypes.string,
      email: PropTypes.string,
    }).isRequired,
    vetting_info: PropTypes.shape({
      quality_tier: PropTypes.string,
      explanation: PropTypes.string,
      composite_score: PropTypes.number,
    }),
    review_status: PropTypes.string, // Added status 
  }).isRequired,
  // Add prop types for the callbacks
  onApprove: PropTypes.func.isRequired,
  onReject: PropTypes.func.isRequired,
  // --- NEW: Prop types for selection --- 
  isSelected: PropTypes.bool.isRequired,
  onSelectChange: PropTypes.func.isRequired,
  // --- NEW: Prop type for active state --- 
  isActive: PropTypes.bool, // Optional, might not be passed initially
};

// Set default prop for isActive
LeadCard.defaultProps = {
    isActive: false,
};

export default LeadCard; 