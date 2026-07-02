import { ResponsiveSankey } from '@nivo/sankey'

interface Props {
  ca: number | null
  marge_brute: number | null
  resultat_net: number | null
}

const fmt = (v: number) =>
  new Intl.NumberFormat('fr-BE', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 }).format(v)

export default function FinancialSankey({ ca, marge_brute, resultat_net }: Props) {
  if (!ca || !marge_brute || !resultat_net) {
    return <p style={{ color: '#888' }}>Données insuffisantes pour le Sankey.</p>
  }

  const couts = ca - marge_brute
  const charges = marge_brute - resultat_net

  if (couts < 0 || charges < 0 || marge_brute <= 0) {
    return <p style={{ color: '#888' }}>Flux financiers négatifs — Sankey non applicable.</p>
  }

  const data = {
    nodes: [
      { id: 'CA', label: `CA\n${fmt(ca)}` },
      { id: 'Marge brute', label: `Marge brute\n${fmt(marge_brute)}` },
      { id: 'Résultat net', label: `Résultat net\n${fmt(resultat_net)}` },
      { id: 'Coûts directs', label: `Coûts directs\n${fmt(couts)}` },
      { id: 'Charges', label: `Charges\n${fmt(charges)}` },
    ],
    links: [
      { source: 'CA', target: 'Marge brute',  value: marge_brute },
      { source: 'CA', target: 'Coûts directs', value: couts },
      { source: 'Marge brute', target: 'Résultat net', value: resultat_net },
      { source: 'Marge brute', target: 'Charges',      value: charges },
    ],
  }

  return (
    <div style={{ height: 300 }}>
      <ResponsiveSankey
        data={data}
        margin={{ top: 10, right: 140, bottom: 10, left: 60 }}
        align="justify"
        colors={{ scheme: 'category10' }}
        nodeOpacity={1}
        nodeBorderRadius={3}
        linkOpacity={0.4}
        enableLinkGradient
        labelPosition="outside"
        labelOrientation="horizontal"
        label={(n) => n.label ?? n.id}
      />
    </div>
  )
}
