import { BrowserRouter, Route, Routes } from 'react-router-dom'
import { Provider } from 'react-redux'
import { store } from './store/store'
import SearchBar from './components/SearchBar'
import EnterprisePage from './pages/EnterprisePage'

export default function App() {
  return (
    <Provider store={store}>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<SearchBar />} />
          <Route path="/enterprise/:bce" element={<EnterprisePage />} />
        </Routes>
      </BrowserRouter>
    </Provider>
  )
}
