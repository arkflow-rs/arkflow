import { Component, ErrorInfo, ReactNode } from 'react'

interface Props {
  children: ReactNode
}

interface State {
  error: Error | null
}

/// Global error boundary: any render-time exception in the console tree
/// shows a recoverable error page instead of unmounting to a white screen.
/// The most realistic trigger is API shape drift (a field the console
/// expects is absent or null from an older Hub).
export class AppErrorBoundary extends Component<Props, State> {
  state: State = { error: null }

  static getDerivedStateFromError(error: Error): State {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('console render error:', error, info.componentStack)
  }

  render() {
    if (this.state.error) {
      const summary = this.state.error.message.slice(0, 200)
      return (
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            justifyContent: 'center',
            minHeight: '100vh',
            gap: '1rem',
            padding: '2rem',
            fontFamily: 'system-ui, sans-serif',
          }}
        >
          <h1 style={{ fontSize: '1.25rem', fontWeight: 600 }}>Something went wrong</h1>
          <p
            style={{
              color: '#666',
              fontSize: '0.875rem',
              maxWidth: '40rem',
              textAlign: 'center',
              wordBreak: 'break-word',
            }}
          >
            {summary}
          </p>
          <button
            onClick={() => window.location.reload()}
            style={{
              padding: '0.5rem 1.5rem',
              borderRadius: '0.375rem',
              border: '1px solid #ccc',
              background: '#f5f5f5',
              cursor: 'pointer',
              fontSize: '0.875rem',
            }}
          >
            Reload
          </button>
        </div>
      )
    }
    return this.props.children
  }
}
