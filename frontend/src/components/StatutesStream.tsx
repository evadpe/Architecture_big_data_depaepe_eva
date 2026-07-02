import { useEffect, useState } from 'react'

interface Statute {
  doc_id: string
  deed_date: string
  title: string
}

export default function StatutesStream({ bce }: { bce: string }) {
  const [statutes, setStatutes] = useState<Statute[]>([])
  const [loading, setLoading]   = useState(true)
  const [error, setError]       = useState<string | null>(null)

  useEffect(() => {
    setStatutes([])
    setLoading(true)
    setError(null)

    const es = new EventSource(`/api/enterprise/${bce}/statutes`)

    es.onmessage = (e) => {
      const data = JSON.parse(e.data)
      if (data.__done__) { setLoading(false); es.close(); return }
      if (data.__error__) { setError(data.__error__); setLoading(false); es.close(); return }
      setStatutes((prev) => [...prev, data as Statute])
    }

    es.onerror = () => { setError('Erreur de connexion SSE'); setLoading(false); es.close() }

    return () => es.close()
  }, [bce])

  return (
    <div>
      <h3 style={{ marginBottom: 12 }}>
        Statuts notaire {loading && <span style={{ fontSize: 13, color: '#888' }}>⏳ chargement…</span>}
      </h3>
      {error && <p style={{ color: '#c0392b' }}>{error}</p>}
      {statutes.length === 0 && !loading && <p style={{ color: '#888' }}>Aucun statut trouvé.</p>}
      <ul style={{ listStyle: 'none' }}>
        {statutes.map((s) => (
          <li key={s.doc_id} style={{ padding: '8px 0', borderBottom: '1px solid #eee' }}>
            <span style={{ color: '#888', fontSize: 13, marginRight: 12 }}>{s.deed_date}</span>
            {s.title || s.doc_id}
          </li>
        ))}
      </ul>
    </div>
  )
}
