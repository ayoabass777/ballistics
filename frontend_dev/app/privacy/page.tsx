import type { Metadata } from 'next';
import Link from 'next/link';
import { ProductHeader } from '@/components/account/ProductHeader';
import s from './privacy.module.css';

export const metadata: Metadata = {
  title: 'Privacy policy | Ballistics',
  description: 'How Ballistics uses account information, Google sign-in data, preferences and beta usage events.',
};

export default function PrivacyPage() {
  return <div className={s.page}>
    <ProductHeader publicPage />
    <main className={s.main}>
      <Link href="/welcome">← Back to Ballistics</Link>
      <h1>Privacy policy</h1>
      <p className={s.date}>Last updated: <time dateTime="2026-10-28">28 October 2026</time></p>
      <p>Ballistics is a football research service. This policy explains how we handle personal information when you visit the site, join the private beta or use an account.</p>

      <h2>Information we collect and why</h2>
      <ul>
        <li><strong>Account and invitation information.</strong> Your email address, account identifier, available name and profile image, and invitation status help us manage beta access and identify your account.</li>
        <li><strong>Your choices.</strong> Saved fixtures, followed leagues, timezone, onboarding progress and usage-event preference let us provide and remember your research experience.</li>
        <li><strong>Beta usage events.</strong> When enabled, we record account-linked interactions such as pages and matches opened, saves and tutorial actions, with timestamps and a session identifier. We use these to understand and improve the product. Event properties do not include names, email addresses or free-text input.</li>
        <li><strong>Technical information.</strong> Hosting and authentication services may process IP addresses, browser information and request logs to deliver the site, diagnose failures and protect access.</li>
      </ul>

      <h2>Google sign-in</h2>
      <p>If you choose Google sign-in, Google and our authentication provider, Clerk, handle authentication. Ballistics uses your account identifier, email address and available basic profile information, including your name and profile image, to create or recognise your account. We do not receive your Google password, and the service does not request access to Gmail messages, Google Drive files, calendars or contacts.</p>
      <p>Google sign-in information is used for account access and account management. We do not sell it or use it for advertising. You can review or remove the connection in your <a href="https://myaccount.google.com/connections">Google Account connections</a>. Removing the connection does not by itself delete information already stored by Ballistics.</p>

      <h2>Cookies and browser storage</h2>
      <p>Authentication uses cookies to keep you signed in and secure your session. Ballistics also uses browser storage to remember interface choices, dismissed tips and your position when returning to a list. Clearing browser storage may reset these choices; blocking authentication cookies can prevent sign-in.</p>

      <h2>Who processes information</h2>
      <p>Clerk processes account and session information for authentication. Our hosting, database, storage and email providers process information needed to operate the service. Google processes information when you choose Google sign-in. These providers may process information in countries other than where you live.</p>
      <p>We do not sell personal information. Access is limited to operating, securing and improving Ballistics, responding to your requests, and disclosures required by law. See <a href="https://clerk.com/legal/privacy">Clerk’s privacy policy</a> and <a href="https://policies.google.com/privacy">Google’s privacy policy</a> for their practices.</p>

      <h2>Retention and your choices</h2>
      <p>We retain account information and preferences while needed to provide your account and operate the service. You can remove saved fixtures, change followed leagues and update preferences in the app.</p>
      <p>You can turn off future beta usage-event collection in Settings → Privacy. We will delete beta usage events within 90 days after the beta ends. Turning collection off does not itself erase previously recorded events.</p>
      <p>You can request access, correction or deletion of your personal information by emailing <a href="mailto:privacy@ballistics.football">privacy@ballistics.football</a>. Deletion requests may require identity verification. Some information may remain in backups until those copies expire or where retention is required for security or legal obligations. Deleting a sign-in account alone does not guarantee immediate removal of separate application records.</p>

      <h2>Contact</h2>
      <p>For privacy questions or requests, email <a href="mailto:privacy@ballistics.football">privacy@ballistics.football</a>.</p>

      <h2>Changes to this policy</h2>
      <p>We will update this page when our practices change and revise the date above. Material changes will be communicated through the service or the contact details associated with your account.</p>
      <footer><Link href="/welcome">Return to Ballistics</Link></footer>
    </main>
  </div>;
}
