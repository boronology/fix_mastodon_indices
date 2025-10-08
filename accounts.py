from typing import LiteralString
from psycopg import Cursor, Connection, connect
from psycopg.rows import TupleRow

from connection_info import ConnectionInfo

# accounts テーブルの referenced by から生成
accounts_relation_queries: tuple[LiteralString, ...] = (
    "UPDATE account_domain_blocks SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE conversation_mutes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE follows SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE blocks SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE reports SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE users SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE favourites SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE follows SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE follow_requests SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE follow_requests SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE blocks SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE media_attachments SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE mentions SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE statuses SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE mutes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE reports SET action_taken_by_account_id = %s WHERE action_taken_by_account_id = ANY(%s)",
    "UPDATE notifications SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE statuses SET in_reply_to_account_id = %s WHERE in_reply_to_account_id = ANY(%s)",
    "UPDATE status_pins SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE reports SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE mutes SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE notifications SET from_account_id = %s WHERE from_account_id = ANY(%s)",
    "UPDATE account_relationship_severance_events SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE tag_follows SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE featured_tags SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE bulk_imports SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE canonical_email_blocks SET reference_account_id = %s WHERE reference_account_id = ANY(%s)",
    "UPDATE account_stats SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE accounts SET moved_to_account_id = %s WHERE moved_to_account_id = ANY(%s)",
    "UPDATE scheduled_statuses SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_statuses_cleanup_policies SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_notes SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE quotes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE lists SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_moderation_notes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_deletion_requests SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE generated_annual_reports SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE reports SET assigned_account_id = %s WHERE assigned_account_id = ANY(%s)",
    "UPDATE account_notes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE appeals SET rejected_by_account_id = %s WHERE rejected_by_account_id = ANY(%s)",
    "UPDATE notification_policies SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE notification_requests SET from_account_id = %s WHERE from_account_id = ANY(%s)",
    "UPDATE polls SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE fasp_follow_recommendations SET recommended_account_id = %s WHERE recommended_account_id = ANY(%s)",
    "UPDATE instance_moderation_notes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_conversations SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE fasp_follow_recommendations SET requesting_account_id = %s WHERE requesting_account_id = ANY(%s)",
    "UPDATE announcement_reactions SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE notification_permissions SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE list_accounts SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE notification_requests SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE custom_filters SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE severed_relationships SET local_account_id = %s WHERE local_account_id = ANY(%s)",
    "UPDATE announcement_mutes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE appeals SET approved_by_account_id = %s WHERE approved_by_account_id = ANY(%s)",
    "UPDATE bookmarks SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_pins SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE account_warnings SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE status_trends SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE admin_action_logs SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_warnings SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE follow_recommendation_mutes SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE poll_votes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE quotes SET quoted_account_id = %s WHERE quoted_account_id = ANY(%s)",
    "UPDATE account_migrations SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE report_notes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE follow_recommendation_mutes SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_pins SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_migrations SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE status_edits SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE preview_cards SET author_account_id = %s WHERE author_account_id = ANY(%s)",
    "UPDATE account_moderation_notes SET target_account_id = %s WHERE target_account_id = ANY(%s)",
    "UPDATE follow_recommendation_suppressions SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE notification_permissions SET from_account_id = %s WHERE from_account_id = ANY(%s)",
    "UPDATE appeals SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE severed_relationships SET remote_account_id = %s WHERE remote_account_id = ANY(%s)",
    "UPDATE tombstones SET account_id = %s WHERE account_id = ANY(%s)",
    "UPDATE account_aliases SET account_id = %s WHERE account_id = ANY(%s)",
)


class AccountsMethod:
    def __init__(self, connection_info: ConnectionInfo):
        self.__connection_info = connection_info

    def __collect_duplicate_accounts(
        self,
        connection: Connection[TupleRow],
    ) -> list[tuple[str, str]]:
        """
        accounts テーブルから同じユーザー名とドメインの組み合わせで重複しているエントリを取得する

        returns: [ユーザー名, ドメイン]のリスト
        """
        with connection.cursor() as cursor:
            _ = cursor.execute("""
                select
                    username
                    , domain
                from (select username, domain, count(*) as c from accounts group by (username, domain)) as dup
                where dup.c > 1
                """)
            return [(item[0], item[1]) for item in cursor.fetchall()]

    def __get_user_ids(
        self, cursor: Cursor[TupleRow], account: tuple[str, str]
    ) -> list[int]:
        """
        ユーザー名とドメインの組から id を取得する。
        このプログラムでは通常、2件以上の id が返る。

        returns: id (int) のリスト
        """
        # (username, domain) = (%s, %s) だとインデクスが使われてしまい、1件のidを取得したところで止まる
        _ = cursor.execute(
            """
            select id from accounts where username || '@' || domain = %s
            """,
            (f"{account[0]}@{account[1]}",),
        )
        return [item[0] for item in cursor.fetchall()]

    def __pre_cleanup(self, cursor: Cursor[TupleRow], user_ids: list[int]) -> None:
        """
        accounts テーブルを参照するテーブルのうち、 account_id の統合で一意性制約に違反するものを修正する
        """
        print("最初のアカウント以外の account_stats を削除", user_ids[1:])
        _ = cursor.execute(
            """
            DELETE FROM account_stats WHERE account_id = ANY(%s)
            """,
            (user_ids[1:],),
        )
        print("tag_id が重複する featured_tags を一意になるよう減らす")
        _ = cursor.execute(
            """
            DELETE FROM featured_tags
            WHERE (account_id, tag_id) IN
                (select account_id, tag_id from
                    (select account_id, tag_id, row_number() over (partition by tag_id) as r from featured_tags where account_id = ANY (%s)) as ft
                where ft.r <> 1)
            """,
            (user_ids,),
        )
        print("target_account_id が重複する follows を一意になるよう減らす")
        _ = cursor.execute(
            """
            DELETE FROM follows
            WHERE (account_id, target_account_id) IN
                (select account_id, target_account_id from
                    (select account_id, target_account_id, row_number() over (partition by account_id) as r from follows where target_account_id = ANY(%s)) as ft
                where ft.r <> 1)
            """,
            (user_ids,),
        )

    def __fix_duplicates_for_user_ids(
        self, cursor: Cursor[TupleRow], user_ids: list[int]
    ) -> None:
        """
        与えられた id をすべて同じユーザーのものとして統合する

        arg:
            user_ids: 同じアカウントに対応する account テーブルの id のリスト
        """
        if len(user_ids) <= 1:
            print("user_idsが1件なので対象外")
            return
        car = user_ids[0]
        cdr = user_ids[1:]
        print("事前にUNIQUEな関連レコードを削除")
        self.__pre_cleanup(cursor, user_ids)
        for q in accounts_relation_queries:
            print("accounts を参照するテーブルのレコードを統合", car, cdr, q)
            _ = cursor.execute(q, (car, cdr))
        print("重複アカウントを削除")
        _ = cursor.execute(
            """
            DELETE FROM accounts WHERE id = ANY(%s)
            """,
            (cdr,),
        )

    def __fix_duplicates_for_account(
        self, connection: Connection[TupleRow], account: tuple[str, str]
    ) -> None:
        """
        与えられたアカウントの重複を探し、1つのアカウントに統合する
        args:
            account: [username, domain] の形式のアカウント情報
        """
        with connection.cursor() as cursor:
            user_ids = self.__get_user_ids(cursor, account)
            if not user_ids:
                print(f"user_idsがないためスキップ: {account[0]}@{account[1]}")
                return
            self.__fix_duplicates_for_user_ids(cursor, user_ids)

    def execute(self):
        """
        エントリポイント
        """
        with connect(self.__connection_info.connection_str) as conn:
            duplicate_accounts = self.__collect_duplicate_accounts(conn)
            for a in duplicate_accounts:
                print(f"重複アカウントの処理: {a[0]}@{a[1]}")
                self.__fix_duplicates_for_account(conn, a)
        print("accounts テーブルの修復おわり")
