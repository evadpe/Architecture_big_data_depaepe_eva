import { configureStore } from '@reduxjs/toolkit'
import enterpriseReducer from './enterpriseSlice'

export const store = configureStore({
  reducer: { enterprise: enterpriseReducer },
})

export type RootState = ReturnType<typeof store.getState>
export type AppDispatch = typeof store.dispatch
