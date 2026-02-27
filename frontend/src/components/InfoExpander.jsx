import React, { useState } from 'react';
import { ChevronDown, ChevronRight, Info } from 'lucide-react';

/**
 * A collapsible expander for surfacing methodology/logic explanations to business users.
 * Usage:
 *   <InfoExpander title="How is Revenue Impact calculated?">
 *     <p>...</p>
 *   </InfoExpander>
 */
export default function InfoExpander({
  title,
  children,
  defaultOpen = false,
  mode = 'collapsible',
}) {
  const [open, setOpen] = useState(defaultOpen);
  const [hoverOpen, setHoverOpen] = useState(false);

  if (mode === 'hover') {
    const isOpen = hoverOpen || open;
    return (
      <div
        style={{ marginTop: 8, position: 'relative', fontSize: '0.82rem' }}
        onMouseEnter={() => setHoverOpen(true)}
        onMouseLeave={() => setHoverOpen(false)}
        onFocusCapture={() => setHoverOpen(true)}
        onBlurCapture={() => setHoverOpen(false)}
      >
        <button
          onClick={() => setOpen(o => !o)}
          aria-expanded={isOpen}
          style={{
            width: '100%',
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            padding: '8px 12px',
            background: 'rgba(139, 148, 158, 0.05)',
            border: '1px solid var(--color-border)',
            borderRadius: 6,
            cursor: 'pointer',
            color: 'var(--color-text-secondary)',
            fontFamily: 'inherit',
            fontSize: '0.8rem',
            textAlign: 'left',
          }}
        >
          <Info size={13} style={{ flexShrink: 0, color: 'var(--color-accent)' }} />
          <span style={{ flex: 1, fontWeight: 500 }}>{title}</span>
          <ChevronDown size={13} style={{ transform: isOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s' }} />
        </button>

        {isOpen && (
          <div
            style={{
              position: 'absolute',
              top: 'calc(100% + 6px)',
              left: 0,
              zIndex: 30,
              width: 'min(460px, calc(100vw - 64px))',
              maxHeight: 280,
              overflowY: 'auto',
              padding: '10px 14px',
              border: '1px solid var(--color-border)',
              borderRadius: 8,
              background: 'var(--color-bg-card)',
              boxShadow: 'var(--shadow-md)',
              color: 'var(--color-text-secondary)',
              lineHeight: 1.55,
            }}
          >
            {children}
          </div>
        )}
      </div>
    );
  }

  return (
    <div style={{
      marginTop: 8,
      border: '1px solid var(--color-border)',
      borderRadius: 6,
      background: 'rgba(139, 148, 158, 0.05)',
      fontSize: '0.82rem',
    }}>
      <button
        onClick={() => setOpen(o => !o)}
        style={{
          width: '100%',
          display: 'flex',
          alignItems: 'center',
          gap: 6,
          padding: '8px 12px',
          background: 'none',
          border: 'none',
          cursor: 'pointer',
          color: 'var(--color-text-secondary)',
          fontFamily: 'inherit',
          fontSize: '0.8rem',
          textAlign: 'left',
        }}
      >
        <Info size={13} style={{ flexShrink: 0, color: 'var(--color-accent)' }} />
        <span style={{ flex: 1, fontWeight: 500 }}>{title}</span>
        {open
          ? <ChevronDown size={13} />
          : <ChevronRight size={13} />
        }
      </button>

      {open && (
        <div style={{
          padding: '0 14px 12px 32px',
          color: 'var(--color-text-secondary)',
          lineHeight: 1.6,
          borderTop: '1px solid var(--color-border)',
        }}>
          {children}
        </div>
      )}
    </div>
  );
}
