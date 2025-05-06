import React from 'react';
import logo from './logo.svg';
import './App.css';
// import CampaignRunner from './components/CampaignRunner'; // Temporarily commented out

function App() {
  return (
    <div className="App">
      <header className="App-header">
        {/* <img src={logo} className="App-logo" alt="logo" /> */}
        <h1>Podcast Vetting System - Review & Demo</h1>
        {/* <p>
          Edit <code>src/App.js</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a> */}
      </header>
      <main>
        {/* <CampaignRunner /> // Temporarily commented out */}
        <p style={{ fontSize: '24px', color: 'red', border: '2px solid blue', padding: '20px' }}>
          TESTING APP RENDER - DO YOU SEE THIS?
        </p>
      </main>
    </div>
  );
}

export default App; 