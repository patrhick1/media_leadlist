import React from 'react';
// Adjust the path relative to app/routes/_index.tsx
// Revert to importing without the extension
import CampaignRunner from '../../src/components/CampaignRunner'; 

// This component will be rendered when the user visits the root URL ("/")
export default function IndexRoute() {
  return <CampaignRunner />;
} 