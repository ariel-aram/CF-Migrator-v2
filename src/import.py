import asyncio
import bz2
import os
import time
from datetime import datetime
from typing import cast

import discord
from tortoise import Tortoise
from tortoise.fields.data import DatetimeField, FloatField, IntField

from ballsdex.core.models import (
    Ball,
    BallInstance,
    BlacklistedGuild,
    BlacklistedID,
    Economy,
    Friendship,
    GuildConfig,
    Player,
    Regime,
    Special,
    Trade,
    TradeObject,
)

__version__ = "1.0.1"

def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def safe_datetime(value):
    if value in (None, "", "None"):
        return None

    if isinstance(value, datetime):
        return value

    try:
        f = float(value)
        if f > 10_000_000_000:
            return datetime.fromtimestamp(f)
        return None
    except (TypeError, ValueError):
        pass

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None

SECTIONS = {
    "R": [Regime, ["id", "background", "name"]],
    "E": [Economy, ["id", "icon", "name"]],
    "S-EV": [
        Special,
        [
            "id",
            "background",
            "catch_phrase",
            "emoji",
            "end_date",
            "hidden",
            "name",
            "rarity",
            "start_date",
            "tradeable",
        ],
    ],
    "S-EX": [
        Special,
        [
            "id",
            "catch_phrase",
            "emoji",
            "background",
            "name",
            "rarity",
        ],
    ],
    "B": [
        Ball,
        [
            "id",
            "capacity_description",
            "capacity_name",
            "credits",
            "regime_id",
            "catch_names",
            "collection_card",
            "economy_id",
            "created_at",
            "emoji_id",
            "enabled",
            "country",
            "attack",
            "rarity",
            "short_name",
            "wild_card",
            "tradeable",
            "health",
        ],
    ],
    "BI": [
        BallInstance,
        [
            "id",
            "ball_id",
            "catch_date",
            "special_id",
            "special_id",
            "favorite",
            "attack_bonus",
            "player_id",
            "server_id",
            "spawned_time",
            "trade_player_id",
            "tradeable",
            "health_bonus",
        ],
    ],
    "P": [
        Player,
        [
            "id",
            "discord_id",
            "donation_policy",
            "privacy_policy",
        ],
    ],
    "GC": [
        GuildConfig,
        [
            "id",
            "enabled",
            "guild_id",
            "spawn_channel",
        ],
    ],
    "F": [
        Friendship,
        [
            "id",
            "player1_id",
            "player2_id",
            "since",
        ],
    ],
    "BU": [
        BlacklistedID,
        [
            "id",
            "date",
            "discord_id",
            "reason",
        ],
    ],
    "BG": [
        BlacklistedGuild,
        [
            "id",
            "date",
            "discord_id",
            "reason",
        ],
    ],
    "T": [
        Trade,
        [
            "id",
            "date",
            "player1_id",
            "player2_id",
        ],
    ],
    "TO": [
        TradeObject,
        [
            "id",
            "ballinstance_id",
            "player_id",
            "trade_id",
        ],
    ],
}


def read_bz2(path: str):
    with bz2.open(path, "rb") as bz2f:
        return bz2f.read().splitlines()


output = []


def reload_embed(start_time: float | None = None, status="RUNNING"):
    embed = discord.Embed(
        title="BD-Migrator Process",
        description=f"Status: **{status}**",
    )

    match status:
        case "RUNNING":
            embed.color = discord.Color.yellow()
        case "FINISHED":
            embed.color = discord.Color.green()
        case "CANCELED":
            embed.color = discord.Color.red()

    if len(output) > 0:
        embed.add_field(name="Output", value="\n".join(output))

    if start_time is not None:
        embed.set_footer(text=f"Ended migration in {round((time.time() - start_time), 3)}s")

    return embed


async def load(message):
    lines = read_bz2("migration.txt.bz2")
    section = ""
    data = {}

    for index, line in enumerate(lines, start=1):
        line = line.decode().rstrip()

        if line.startswith("//") or line == "":
            continue

        if line.startswith(":"):
            section = line[1:]

            if section not in SECTIONS:
                raise Exception(f"Invalid section '{section}' detected on line {index}")

            continue

        if section == "":
            continue

        section_full = SECTIONS[section]

        if section_full[0] not in data:
            data[section_full[0]] = []

        model_dict = {}
        fields = section_full[0]._meta.fields_map
        attribute_index = 0

        for value, line_data in zip(section_full[1], line.split("╵")):
            attribute_index += 1

            if line_data == "":
                continue

            if value not in fields:
                raise Exception(
                    f"Uknown value '{value}' detected on line {index:,} - "
                    f"attribute {attribute_index:,} in {section_full[0].__name__} object"
                )

            if line_data == "None":
                line_data = None
            elif line_data == "🬀":
                line_data = True
            elif line_data == "🬁":
                line_data = False

            field_type = fields[value]

            if line_data is not None:
                if isinstance(field_type, IntField):
                    line_data = safe_int(line_data)
                elif isinstance(field_type, FloatField):
                    line_data = float(line_data)
                elif isinstance(field_type, DatetimeField):
                    line_data = safe_datetime(line_data)

            if isinstance(line_data, str):
                line_data = line_data.replace("🮈", "\n")

            model_dict[value] = line_data

        data[section_full[0]].append(model_dict)

    start_time = time.time()

    for item, value in data.items():
        items = []

        for model in value:
            items.append(item(**model))

        await item.bulk_create(items)

        output.append(f"- Added **{len(value):,}** {item.__name__} objects.")

        await message.edit(embed=reload_embed())

    await sequence_all_models()

    await message.edit(embed=reload_embed(start_time, "FINISHED"))


async def sequence_model(model):
    if await model.all().count() == 0:
        return

    client = Tortoise.get_connection("default")

    last_id = await model.all().order_by("-id").first().values_list("id", flat=True)

    await client.execute_query(f"SELECT setval('{model._meta.db_table}_id_seq', {last_id});")


async def sequence_all_models():
    models = Tortoise.apps.get("models")

    if models is None:
        return

    for model in models.values():
        await sequence_model(model)


async def clear_all_data():  # I'm not responsible if any of you eval goblins run this on your dex
    models = Tortoise.apps.get("models")

    if models is None:
        return

    await TradeObject.all().delete()
    await Trade.all().delete()
    await BallInstance.all().delete()

    for model in models.values():
        await model.all().delete()


async def main():
    if os.path.isdir("carfigures"):
        print("You cannot run this command from CarFigures.")
        return

    if not os.path.isfile("migration.txt.bz2"):
        print("Could not find `migration.txt.bz2` migration file.")
        return

    try:
        await ctx.send(  # type: ignore # noqa: F821
            "**WARNING**: All existing data on this bot will be **CLEARED**.\n"
            "Type `proceed` if you wish to proceed.\n"
            "Type `cancel` if you wish to cancel."
        )

        confirm_message = await bot.wait_for(  # type: ignore # noqa: F821
            "message",
            check=lambda m: m.author == ctx.author  # type: ignore # noqa: F821
            and m.channel == ctx.channel  # type: ignore # noqa: F821
            and m.content.lower() in ["proceed", "cancel"],
            timeout=20,
        )
    except asyncio.TimeoutError:
        await ctx.send("Canceled due to response timeout.")  # type: ignore # noqa: F821
        return

    if confirm_message.content.lower() != "proceed":
        await ctx.send("Canceled due to message response.")  # type: ignore # noqa: F821
        return

    message = await ctx.send(embed=reload_embed())  # type: ignore # noqa: F821

    await clear_all_data()
    await load(message)


await main()  # type: ignore  # noqa: F704

