from fastapi.testclient import TestClient

from API.app import app, get_current_user_claims, get_db


class Result:
    def __init__(self, value=None):
        self.value = value

    def scalar_one(self):
        return self.value

    def scalars(self):
        return self

    def all(self):
        return self.value


class DB:
    def __init__(self, complete=False):
        self.complete = complete
        self.follows = []
        self.commits = 0
        self.rollbacks = 0

    def execute(self, query, params=None):
        sql = ' '.join(str(query).split())
        if 'SELECT onboarding_completed' in sql:
            return Result(self.complete)
        if 'SELECT league_id FROM dim.dim_leagues' in sql:
            return Result([i for i in params['ids'] if i in (1, 2)])
        if 'DELETE FROM app.user_followed_leagues' in sql:
            self.follows = [i for i in self.follows if i in params['ids']]
        if 'INSERT INTO app.user_followed_leagues' in sql:
            self.follows.append(params['league_id'])
        if 'UPDATE app.app_users SET onboarding_completed' in sql:
            self.complete = True
        return Result()

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_first_run_saves_leagues_then_cannot_overwrite_on_replay():
    db = DB()
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_current_user_claims] = lambda: {'sub': 'new_member'}
    try:
        client = TestClient(app)
        result = client.post('/account/onboarding/complete', json={'league_ids': [1, 2]})
        assert result.status_code == 200
        assert result.json() == {'data': {'onboarding_completed': True}}
        assert db.follows == [1, 2]
        assert db.complete
        assert client.post('/account/onboarding/complete', json={'league_ids': []}).status_code == 200
        assert db.follows == [1, 2]
    finally:
        app.dependency_overrides.clear()


def test_unknown_league_does_not_complete_onboarding():
    db = DB()
    app.dependency_overrides[get_db] = lambda: db
    app.dependency_overrides[get_current_user_claims] = lambda: {'sub': 'new_member'}
    try:
        result = TestClient(app).post('/account/onboarding/complete', json={'league_ids': [99]})
        assert result.status_code == 422
        assert not db.complete and not db.follows
        assert db.rollbacks == 1
    finally:
        app.dependency_overrides.clear()
