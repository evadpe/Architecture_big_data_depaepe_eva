import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { useDispatch, useSelector } from 'react-redux'
import { fetchEnterprise } from '../store/enterpriseSlice'
import type { AppDispatch, RootState } from '../store/store'
import FinancialSankey from '../components/FinancialSankey'
import RatiosTable from '../components/RatiosTable'
import StatutesStream from '../components/StatutesStream'

export default function EnterprisePage() {
  const { bce }    = useParams<{ bce: string }>()
  const dispatch   = useDispatch<AppDispatch>()
  const navigate   = useNavigate()
  const { current, loading, error } = useSelector((s: RootState) => s.enterprise)
  const [selectedYear, setSelectedYear] = useState<number | null>(null)

  useEffect(() => { if (bce) dispatch(fetchEnterprise(bce)) }, [bce, dispatch])

  if (loading) return <p style={{ padding: 32 }}>Chargement…</p>
  if (error)   return <p style={{ padding: 32, color: 'red' }}>{error}</p>
  if (!current) return null

  const { silver, gold } = current
  const denoms  = silver?.denominations ?? []
  const name    = denoms[0]?.denomination ?? bce
  const address = (silver?.addresses ?? [])[0]
  const activities = silver?.activities ?? []
  const years   = gold?.years ?? []
  const yearData = years.find((y: any) => y.year === selectedYear) ?? years[years.length - 1]

  return (
    <div style={{ maxWidth: 900, margin: '0 auto', padding: '24px 16px' }}>
      <button onClick={() => navigate(-1)} style={{ marginBottom: 16, background: 'none', border: 'none', cursor: 'pointer', color: '#1a1a2e', fontSize: 14 }}>
        ← Retour
      </button>

      {/* En-tête */}
      <div style={{ background: '#1a1a2e', color: '#fff', borderRadius: 10, padding: '24px 28px', marginBottom: 20 }}>
        <h1 style={{ fontSize: 22, marginBottom: 6 }}>{name}</h1>
        <p style={{ opacity: .7, fontSize: 14 }}>{bce} · {silver?.JuridicalFormLabel} · <span style={{ color: (silver?.Status === 'AC') ? '#2ecc71' : '#e74c3c' }}>{silver?.StatusLabel ?? silver?.Status}</span></p>
        {address && (
          <p style={{ marginTop: 8, opacity: .8, fontSize: 14 }}>
            {address.street_fr} {address.house_number}, {address.zipcode} {address.municipality_fr}
          </p>
        )}
      </div>

      {/* Activités NACE */}
      {activities.length > 0 && (
        <section style={card}>
          <h2 style={cardTitle}>Activités NACE</h2>
          <ul style={{ listStyle: 'none' }}>
            {activities.map((a: any, i: number) => (
              <li key={i} style={{ padding: '4px 0', fontSize: 14 }}>
                <strong>{a.nace_code}</strong> — {a.NaceLabel ?? '—'} <span style={{ color: '#999' }}>({a.classification})</span>
              </li>
            ))}
          </ul>
        </section>
      )}

      {/* Sankey + sélecteur année */}
      {gold && years.length > 0 && (
        <section style={card}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
            <h2 style={cardTitle}>Compte de résultats</h2>
            <select
              value={selectedYear ?? yearData?.year ?? ''}
              onChange={(e) => setSelectedYear(Number(e.target.value))}
              style={{ padding: '6px 10px', borderRadius: 4, border: '1px solid #ccc' }}
            >
              {[...years].sort((a: any, b: any) => b.year - a.year).map((y: any) => (
                <option key={y.year} value={y.year}>{y.year}</option>
              ))}
            </select>
          </div>
          <FinancialSankey
            ca={yearData?.chiffre_affaires}
            marge_brute={yearData?.ratios?.marge_brute}
            resultat_net={yearData?.resultat_net}
          />
        </section>
      )}

      {/* Tableau des ratios */}
      {gold && years.length > 0 && (
        <section style={card}>
          <h2 style={{ ...cardTitle, marginBottom: 16 }}>Ratios financiers par exercice</h2>
          <RatiosTable years={years} />
        </section>
      )}

      {!gold && (
        <section style={card}>
          <p style={{ color: '#888' }}>Aucune donnée financière disponible pour cette entreprise.</p>
        </section>
      )}

      {/* Statuts notaire SSE */}
      <section style={card}>
        {bce && <StatutesStream bce={bce} />}
      </section>
    </div>
  )
}

const card: React.CSSProperties = {
  background: '#fff', borderRadius: 10, padding: '20px 24px',
  marginBottom: 16, boxShadow: '0 1px 4px rgba(0,0,0,.06)',
}
const cardTitle: React.CSSProperties = { fontSize: 16, fontWeight: 600, marginBottom: 0 }
