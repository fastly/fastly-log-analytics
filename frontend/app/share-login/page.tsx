import * as React from 'react'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { KeyRound } from 'lucide-react'
import { ShareLoginForm } from './ShareLoginForm'

export default function ShareLoginPage() {
  return (
    <div className="min-h-screen flex justify-center bg-muted/40 p-6 pt-20">
      <Card className="w-full max-w-md">
        <CardHeader className="space-y-2">
          <CardTitle className="flex items-center gap-2 text-xl">
            <KeyRound className="h-5 w-5" />
            Analyst sign-in
          </CardTitle>
          <p className="text-sm text-muted-foreground">
            Access to this dashboard is invite-only. Enter the email address the
            invite was sent to, and the passcode from the invitation message. If
            you don&apos;t have an invite, ask the dashboard owner to send you one.
          </p>
        </CardHeader>
        <CardContent>
          <ShareLoginForm />
        </CardContent>
      </Card>
    </div>
  )
}
