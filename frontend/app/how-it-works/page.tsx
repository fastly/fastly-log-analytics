import type { Metadata } from 'next'
import { IS_CLOUD_MODE } from '@/lib/cloud-engine/mode'
import { CloudHowItWorks } from '@/components/how-it-works/cloud'
import { HighScaleHowItWorks } from '@/components/how-it-works/high-scale'

export const metadata: Metadata = { title: 'How it works · Fastly Log Analytics' }

export default function HowItWorksPage() {
  return IS_CLOUD_MODE ? <CloudHowItWorks /> : <HighScaleHowItWorks />
}
