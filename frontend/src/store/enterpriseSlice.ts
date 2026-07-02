import { createAsyncThunk, createSlice } from '@reduxjs/toolkit'
import axios from 'axios'

const API = '/api'

export const searchEnterprises = createAsyncThunk(
  'enterprise/search',
  async (q: string) => (await axios.get(`${API}/search`, { params: { q } })).data
)

export const fetchEnterprise = createAsyncThunk(
  'enterprise/fetch',
  async (bce: string) => (await axios.get(`${API}/enterprise/${bce}`)).data
)

interface EnterpriseState {
  results: any[]
  current: any | null
  loading: boolean
  error: string | null
}

const initial: EnterpriseState = { results: [], current: null, loading: false, error: null }

const slice = createSlice({
  name: 'enterprise',
  initialState: initial,
  reducers: { clearCurrent: (s) => { s.current = null } },
  extraReducers: (b) => {
    b.addCase(searchEnterprises.pending,   (s) => { s.loading = true; s.error = null })
     .addCase(searchEnterprises.fulfilled, (s, a) => { s.loading = false; s.results = a.payload })
     .addCase(searchEnterprises.rejected,  (s, a) => { s.loading = false; s.error = a.error.message ?? 'Erreur' })
     .addCase(fetchEnterprise.pending,     (s) => { s.loading = true; s.error = null })
     .addCase(fetchEnterprise.fulfilled,   (s, a) => { s.loading = false; s.current = a.payload })
     .addCase(fetchEnterprise.rejected,    (s, a) => { s.loading = false; s.error = a.error.message ?? 'Erreur' })
  },
})

export const { clearCurrent } = slice.actions
export default slice.reducer
