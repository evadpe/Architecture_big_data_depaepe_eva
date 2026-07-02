import { useState, useCallback } from 'react'
import { useDispatch, useSelector } from 'react-redux'
import { useNavigate } from 'react-router-dom'
import { searchEnterprises } from '../store/enterpriseSlice'
import type { AppDispatch, RootState } from '../store/store'

export default function SearchBar() {
  const [q, setQ] = useState('')
  const dispatch   = useDispatch<AppDispatch>()
  const navigate   = useNavigate()
  const { results, loading } = useSelector((s: RootState) => s.enterprise)

  const search = useCallback(() => {
    if (q.trim().length >= 2) dispatch(searchEnterprises(q.trim()))
  }, [q, dispatch])

  return (
    <div style={{ maxWidth: 700, margin: '60px auto', padding: '0 16px' }}>
      <h1 style={{ marginBottom: 24, color: '#1a1a2e' }}>Entreprises Hébergement</h1>
      <div style={{ display: 'flex', gap: 8 }}>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && search()}
          placeholder="Nom ou numéro BCE…"
          style={{ flex: 1, padding: '10px 14px', fontSize: 16, border: '1px solid #ccc', borderRadius: 6 }}
        />
        <button
          onClick={search}
          style={{ padding: '10px 20px', background: '#1a1a2e', color: '#fff', border: 'none', borderRadius: 6, cursor: 'pointer', fontSize: 16 }}
        >
          {loading ? '…' : 'Chercher'}
        </button>
      </div>

      {results.length > 0 && (
        <ul style={{ marginTop: 16, listStyle: 'none', background: '#fff', borderRadius: 8, boxShadow: '0 2px 8px rgba(0,0,0,.08)' }}>
          {results.map((r) => (
            <li
              key={r.bce}
              onClick={() => navigate(`/enterprise/${r.bce}`)}
              style={{ padding: '12px 16px', borderBottom: '1px solid #f0f0f0', cursor: 'pointer', display: 'flex', justifyContent: 'space-between' }}
            >
              <span><strong>{r.name || '—'}</strong><br /><small style={{ color: '#666' }}>{r.bce}</small></span>
              <span style={{ color: '#888', fontSize: 13 }}>{r.form}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
