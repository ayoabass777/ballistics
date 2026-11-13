import Link from 'next/link';
import { matchHref, type Card } from '@/lib/research';
import { Button, Crest } from './shared';
import styles from './LeagueFixtureRows.module.css';

type Props = {
    fixtures: Card[];
    zone: string;
    returnTo: string;
    isSaved: (id: number) => boolean;
    onSave: (card: Card) => void;
};

export function LeagueFixtureRows({ fixtures, zone, returnTo, isSaved, onSave }: Props) {
    const leagues = new Map<string, Card[]>();
    for (const fixture of fixtures) {
        const key = String(fixture.league_id ?? JSON.stringify([fixture.country, fixture.league]));
        const group = leagues.get(key);
        if (group) group.push(fixture);
        else leagues.set(key, [fixture]);
    }
    const clock = new Intl.DateTimeFormat('en-GB', { timeZone: zone, hour: '2-digit', minute: '2-digit' });
    return <div className={styles.groups}>{Array.from(leagues, ([key, group]) => {
        const first = group[0];
        return <section key={key} className={styles.group}>
            <h4><Crest name={first.league} url={first.league_logo_url} league/>
                {[first.country, first.league].filter(Boolean).join(' · ')}</h4>
            {group.map(card => <div key={card.fixture_id} className={styles.row}>
                <Link href={matchHref(card, false, returnTo)} className={styles.fixture} aria-label={card.home + ' vs ' + card.away + ' match details'}>
                    <time dateTime={card.kickoff}>{clock.format(new Date(card.kickoff))}</time>
                    <span className={styles.teams}>
                        <span><Crest name={card.home} url={card.home_crest_url}/>{card.home}</span>
                        <span><Crest name={card.away} url={card.away_crest_url}/>{card.away}</span>
                    </span>
                </Link>
                <Button aria-label={'Save ' + card.home + ' vs ' + card.away} aria-pressed={isSaved(card.fixture_id)} onClick={() => onSave(card)}>{isSaved(card.fixture_id) ? '✓ Saved' : 'Save'}</Button>
            </div>)}
        </section>;
    })}</div>;
}
