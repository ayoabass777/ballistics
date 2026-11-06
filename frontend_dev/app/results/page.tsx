import { redirect } from 'next/navigation';
// Results moved into Matches: past days show their played games there.
export default function Page() { redirect('/matches'); }
