import { Suspense } from 'react'

import ControlRoomLoading from './loading'
import ControlRoomClient from './_sections/ControlRoomClient'

export default function ControlRoomPage() {
  return (
    <Suspense fallback={<ControlRoomLoading />}>
      <ControlRoomClient />
    </Suspense>
  )
}
