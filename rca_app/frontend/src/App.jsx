import React from 'react';
import { Routes, Route, NavLink, useLocation } from 'react-router-dom';
import {
  LayoutDashboard, Search, BarChart3, GitCompare,
  Layers, Network, Activity, Sparkles
} from 'lucide-react';

import ExecutiveDashboard from './pages/ExecutiveDashboard';
import RootCauseIntelligence from './pages/RootCauseIntelligence';
import ServiceRiskRanking from './pages/ServiceRiskRanking';
import ChangeCorrelation from './pages/ChangeCorrelation';
import DomainDeepDive from './pages/DomainDeepDive';
import TopologyExplorer from './pages/TopologyExplorer';
import GenieChat from './pages/GenieChat';

const NAV_ITEMS = [
  {
    section: 'Overview',
    items: [
      { path: '/', label: 'Executive Dashboard', icon: LayoutDashboard },
      { path: '/root-cause', label: 'Root Cause Intelligence', icon: Search },
    ],
  },
  {
    section: 'Analysis',
    items: [
      { path: '/service-risk', label: 'Service Risk Ranking', icon: BarChart3 },
      { path: '/change-correlation', label: 'Change Correlation', icon: GitCompare },
    ],
  },
  {
    section: 'Explore',
    items: [
      { path: '/domain-deep-dive', label: 'Domain Deep Dive', icon: Layers },
      { path: '/topology', label: 'Topology Explorer', icon: Network },
    ],
  },
  {
    section: 'Investigate',
    items: [
      { path: '/genie', label: 'Ask Genie', icon: Sparkles },
    ],
  },
];

export default function App() {
  const location = useLocation();

  return (
    <div className="app-layout">
      {/* Sidebar */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <Activity size={20} style={{ color: 'var(--color-accent)' }} />
            <h1>RCA Intelligence</h1>
          </div>
          <div className="subtitle">Enterprise Root Cause Analysis</div>
        </div>
        <nav>
          <ul className="sidebar-nav">
            {NAV_ITEMS.map((section) => (
              <React.Fragment key={section.section}>
                <li className="sidebar-section-title">{section.section}</li>
                {section.items.map((item) => {
                  const Icon = item.icon;
                  const isActive = location.pathname === item.path;
                  return (
                    <li key={item.path}>
                      <NavLink
                        to={item.path}
                        className={isActive ? 'active' : ''}
                      >
                        <Icon className="nav-icon" size={18} />
                        {item.label}
                      </NavLink>
                    </li>
                  );
                })}
              </React.Fragment>
            ))}
          </ul>
        </nav>
        <div style={{
          position: 'absolute', bottom: 0, left: 0, right: 0,
          padding: '12px 24px',
          borderTop: '1px solid var(--color-border)',
          fontSize: '0.7rem',
          color: 'var(--color-text-muted)',
        }}>
          <div>JnJ Enterprise Observability</div>
          <div style={{ marginTop: 2 }}>OpenTelemetry + Databricks</div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="main-content">
        <Routes>
          <Route path="/" element={<ExecutiveDashboard />} />
          <Route path="/root-cause" element={<RootCauseIntelligence />} />
          <Route path="/service-risk" element={<ServiceRiskRanking />} />
          <Route path="/change-correlation" element={<ChangeCorrelation />} />
          <Route path="/domain-deep-dive" element={<DomainDeepDive />} />
          <Route path="/topology" element={<TopologyExplorer />} />
          <Route path="/genie" element={<GenieChat />} />
        </Routes>
      </main>
    </div>
  );
}
