interface YearData {
  year: number
  chiffre_affaires: number | null
  ebit: number | null
  resultat_net: number | null
  fonds_propres: number | null
  tresorerie: number | null
  dettes_financieres: number | null
  ratios: {
    marge_brute: number | null
    marge_nette_pct: number | null
    roe_pct: number | null
    ratio_liquidite: number | null
    taux_endettement_pct: number | null
  }
}

const fmt = (v: number | null, suffix = '') =>
  v == null ? '—' : `${new Intl.NumberFormat('fr-BE', { maximumFractionDigits: 1 }).format(v)}${suffix}`

const fmtEur = (v: number | null) =>
  v == null ? '—' : new Intl.NumberFormat('fr-BE', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }).format(v)

export default function RatiosTable({ years }: { years: YearData[] }) {
  const sorted = [...years].sort((a, b) => b.year - a.year)

  return (
    <div style={{ overflowX: 'auto' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 14 }}>
        <thead>
          <tr style={{ background: '#1a1a2e', color: '#fff' }}>
            <th style={th}>Exercice</th>
            <th style={th}>CA</th>
            <th style={th}>EBIT</th>
            <th style={th}>Résultat net</th>
            <th style={th}>Marge brute</th>
            <th style={th}>Marge nette %</th>
            <th style={th}>ROE %</th>
            <th style={th}>Liquidité</th>
            <th style={th}>Endettement %</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((y) => (
            <tr key={y.year} style={{ borderBottom: '1px solid #eee' }}>
              <td style={td}><strong>{y.year}</strong></td>
              <td style={td}>{fmtEur(y.chiffre_affaires)}</td>
              <td style={td}>{fmtEur(y.ebit)}</td>
              <td style={{ ...td, color: (y.resultat_net ?? 0) < 0 ? '#c0392b' : '#27ae60' }}>
                {fmtEur(y.resultat_net)}
              </td>
              <td style={td}>{fmtEur(y.ratios?.marge_brute)}</td>
              <td style={td}>{fmt(y.ratios?.marge_nette_pct, ' %')}</td>
              <td style={td}>{fmt(y.ratios?.roe_pct, ' %')}</td>
              <td style={td}>{fmt(y.ratios?.ratio_liquidite)}</td>
              <td style={td}>{fmt(y.ratios?.taux_endettement_pct, ' %')}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

const th: React.CSSProperties = { padding: '10px 12px', textAlign: 'right', fontWeight: 600 }
const td: React.CSSProperties = { padding: '8px 12px', textAlign: 'right' }
