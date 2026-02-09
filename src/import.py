import asyncio
import bz2
import os
import time
from datetime import datetime, date
from typing import cast

import discord
from tortoise import Tortoise
from tortoise.fields.data import DatetimeField, DateField, FloatField, IntField
from tortoise.exceptions import ValidationError  # Claude AI - Added for error handling

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

__version__ = "1.0.2-with-placeholders"  # Claude AI - Version marker

# ----------- ChatGPT Starts Here -------------
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

        if 0 <= f <= 4_102_444_800:
            return datetime.fromtimestamp(f)
    except (TypeError, ValueError, OSError):
        pass

    try:
        return datetime.fromisoformat(str(value))
    except (ValueError, TypeError):
        return None
    
def safe_date(value):
    if value in (None, "", "None"):
        return None

    if isinstance(value, date):
        return value

    try:
        f = float(value)
        if f > 10_000_000_000:
            return date.fromtimestamp(f)
        return None
    except (TypeError, ValueError):
        pass

    try:
        return date.fromisoformat(value)
    except ValueError:
        return None

# ----------- ChatGPT Ends Here -------------

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
        # Claude AI - Only show last 20 lines to avoid Discord's 1024 char limit
        recent_output = output[-20:] if len(output) > 20 else output
        output_text = "\n".join(recent_output)
        
        # Claude AI - If still too long, truncate
        if len(output_text) > 1000:
            output_text = "...\n" + output_text[-1000:]
        
        embed.add_field(name="Output", value=output_text)

    if start_time is not None:
        embed.set_footer(text=f"Ended migration in {round((time.time() - start_time), 3)}s")

    return embed


async def get_or_create_placeholder_player(missing_player_id, placeholder_log, created_placeholders):
    """
    Create a unique placeholder Player for a specific missing player ID.
    Returns the NEW database ID (pk) of the placeholder.
    """
    # Claude AI - Check if we already created this placeholder
    placeholder_key = f"Player_{missing_player_id}"
    if placeholder_key in created_placeholders:
        return created_placeholders[placeholder_key]
    
    # Claude AI - Use a large negative offset to indicate placeholder
    # Original player_id=123 becomes discord_id=-10000000123
    placeholder_discord_id = -10000000000 - missing_player_id
    
    placeholder_player = await Player.filter(discord_id=placeholder_discord_id).first()
    if not placeholder_player:
        placeholder_player = await Player.create(
            discord_id=placeholder_discord_id,
            donation_policy=0,
            privacy_policy=0
        )
        placeholder_log.write(f"Created placeholder Player (discord_id={placeholder_discord_id}, DB ID={placeholder_player.pk}) for missing Player ID {missing_player_id}\n")
    
    # Claude AI - Cache and return the actual database PK
    created_placeholders[placeholder_key] = placeholder_player.pk
    return placeholder_player.pk


async def load(message):
    lines = read_bz2("migration.txt.bz2")
    section = ""
    data = {}

    # Claude AI - Open a log file for skipped records (use current directory)
    skipped_log = open("skipped_records.log", "w", encoding="utf-8")
    skipped_log.write("=== MIGRATION SKIPPED RECORDS LOG ===\n")
    skipped_log.write(f"Generated: {datetime.now()}\n\n")
    
    # Claude AI - Open a log file for placeholder assignments
    placeholder_log = open("placeholder_assignments.log", "w", encoding="utf-8")
    placeholder_log.write("=== PLACEHOLDER ASSIGNMENTS LOG ===\n")
    placeholder_log.write(f"Generated: {datetime.now()}\n")
    placeholder_log.write("Records assigned to placeholder entities (grouped by original missing reference):\n\n")
    
    # Claude AI - Track created placeholders to avoid recreating them
    created_placeholders = {}

    output.append(f"- Reading migration file with {len(lines):,} lines...")  # Claude AI - Progress message
    await message.edit(embed=reload_embed())

    for index, line in enumerate(lines, start=1):
        line = line.decode().rstrip()

        # Claude AI - Progress update every 10000 lines
        if index % 10000 == 0:
            output[-1] = f"- Reading migration file... (line {index:,}/{len(lines):,})"
            await message.edit(embed=reload_embed())

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

            # Claude AI - Special handling for id field - it must never be empty
            if value == "id" and line_data == "":
                # Skip this entire record if id is empty
                skipped_log.write(f"Line {index} - {section_full[0].__name__}: SKIPPED - Empty ID field\n")
                model_dict = None
                break
            
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
                elif isinstance(field_type, DatetimeField): # ChatGPT
                    line_data = safe_datetime(line_data) # ChatGPT
                elif isinstance(field_type, DateField): # ChatGPT
                    line_data = safe_date(line_data) # ChatGPT

            if isinstance(line_data, str):
                line_data = line_data.replace("🮈", "\n")

            model_dict[value] = line_data

        # Claude AI - Only add the record if it has a valid id
        if model_dict is not None:
            data[section_full[0]].append(model_dict)

    output.append(f"- Finished reading migration file. Processing {len(data)} model types...")  # Claude AI - Progress message
    await message.edit(embed=reload_embed())

    start_time = time.time()

    # Claude AI - Process each model type separately and handle duplicates
    # Claude AI - Track inserted IDs for foreign key validation
    inserted_ids = {}
    
    for item, value in data.items():
        output.append(f"- Processing {item.__name__}... ({len(value):,} records to validate)")  # Claude AI - Progress message
        await message.edit(embed=reload_embed())
        
        # Claude AI - Get field definitions to check which fields are required
        fields_map = item._meta.fields_map
        
        # Claude AI - Identify foreign key fields for this model
        fk_fields = {}
        for field_name, field_obj in fields_map.items():
            # Check if this is a ForeignKeyField
            if hasattr(field_obj, 'related_model') and field_obj.related_model is not None:
                fk_fields[field_name] = field_obj.related_model
        
        # Claude AI - Remove duplicates based on ID and skip records with invalid data
        seen_ids = set()
        unique_values = []
        skipped_count = 0
        fk_violation_count = 0
        null_field_count = 0
        duplicate_count = 0
        
        for idx, model in enumerate(value):
            # Claude AI - Progress update every 5000 records during validation
            if idx > 0 and idx % 5000 == 0:
                output[-1] = f"- Processing {item.__name__}... (validated {idx:,}/{len(value):,})"
                await message.edit(embed=reload_embed())
            
            model_id = model.get('id')
            
            # Claude AI - Skip records with None/null id
            if model_id is None:
                skipped_log.write(f"{item.__name__} - ID: None - SKIPPED: Null ID\n")
                skipped_count += 1
                continue
            
            # Claude AI - Skip duplicate IDs
            if model_id in seen_ids:
                skipped_log.write(f"{item.__name__} - ID: {model_id} - SKIPPED: Duplicate ID\n")
                skipped_count += 1
                duplicate_count += 1
                continue
            
            # Claude AI - Validate foreign key references and create placeholders if needed
            has_invalid_fk = False
            for fk_field_name, related_model in fk_fields.items():
                fk_value = model.get(fk_field_name)
                if fk_value is not None:
                    # Claude AI - Check if the referenced ID exists either in inserted_ids OR in the database
                    exists_in_tracking = related_model in inserted_ids and fk_value in inserted_ids[related_model]
                    
                    if not exists_in_tracking:
                        # Claude AI - Check if it exists in the actual database
                        exists_in_db = await related_model.filter(pk=fk_value).exists()
                        
                        if not exists_in_db:
                            # Claude AI - Only create placeholder if it truly doesn't exist anywhere
                            if related_model == Player:
                                # Create placeholder and get its NEW database ID
                                placeholder_id = await get_or_create_placeholder_player(fk_value, placeholder_log, created_placeholders)
                                
                                # Add to inserted_ids so future records can reference it
                                if Player not in inserted_ids:
                                    inserted_ids[Player] = set()
                                inserted_ids[Player].add(placeholder_id)
                                
                                # Update the model to use the placeholder's NEW ID
                                model[fk_field_name] = placeholder_id
                                placeholder_log.write(f"{item.__name__} ID {model_id}: Reassigned {fk_field_name} from missing Player ID {fk_value} to placeholder DB ID {placeholder_id}\n")
                            else:
                                # For other models, still skip
                                skipped_log.write(f"{item.__name__} - ID: {model_id} - SKIPPED: Invalid FK {fk_field_name}={fk_value} (references non-existent {related_model.__name__})\n")
                                has_invalid_fk = True
                                fk_violation_count += 1
                                break
            
            if has_invalid_fk:
                skipped_count += 1
                continue
            
            # Claude AI - Check for None values in non-nullable fields
            skip_record = False
            null_fields = []
            for field_name, field_value in model.items():
                if field_value is None and field_name in fields_map:
                    field_obj = fields_map[field_name]
                    # Check if field is required (not null and not a relation field)
                    if hasattr(field_obj, 'null') and not field_obj.null:
                        null_fields.append(field_name)
                        skip_record = True
            
            if skip_record:
                skipped_log.write(f"{item.__name__} - ID: {model_id} - SKIPPED: Null required fields: {', '.join(null_fields)}\n")
                skipped_count += 1
                null_field_count += 1
                continue
                
            seen_ids.add(model_id)
            unique_values.append(model)
        
        output[-1] = f"- Creating {item.__name__} instances... ({len(unique_values):,} valid records)"  # Claude AI - Progress message
        await message.edit(embed=reload_embed())
        
        # Claude AI - Create model instances with additional error handling
        items = []
        validation_fail_count = 0
        for idx, model in enumerate(unique_values):
            # Claude AI - Progress update every 5000 records during instance creation
            if idx > 0 and idx % 5000 == 0:
                output[-1] = f"- Creating {item.__name__} instances... ({idx:,}/{len(unique_values):,})"
                await message.edit(embed=reload_embed())
            
            try:
                instance = item(**model)
                items.append(instance)
            except (ValueError, ValidationError) as e:
                # Claude AI - Skip records that fail validation
                skipped_log.write(f"{item.__name__} - ID: {model.get('id')} - SKIPPED: Validation error: {str(e)[:200]}\n")
                skipped_count += 1
                validation_fail_count += 1
                continue

        output[-1] = f"- Saving {item.__name__} to database... ({len(items):,} objects)"  # Claude AI - Progress message
        await message.edit(embed=reload_embed())

        if items:  # Claude AI - Only bulk_create if we have items
            try:
                await item.bulk_create(items)
                
                # Claude AI - Track successfully inserted IDs for foreign key validation
                inserted_ids[item] = seen_ids
                
            except Exception as e:
                # Claude AI - Log the actual error to help debug
                error_msg = f"ERROR: {type(e).__name__}: {str(e)[:200]}"
                skipped_log.write(f"\n{item.__name__} BULK CREATE FAILED: {error_msg}\n")
                output.append(f"- Bulk create failed for {item.__name__}: {error_msg}")
                output.append(f"- Attempting individual saves for {len(items):,} records (THIS WILL BE SLOW)...")
                await message.edit(embed=reload_embed())
                
                success_count = 0
                successful_ids = set()
                individual_fail_count = 0
                
                for idx, single_item in enumerate(items):
                    if idx > 0 and idx % 1000 == 0:
                        output[-1] = f"- Saving {item.__name__} individually... ({idx:,}/{len(items):,} - ~{int((idx/len(items))*100)}% done)"
                        await message.edit(embed=reload_embed())
                    
                    try:
                        await single_item.save()
                        success_count += 1
                        # Track the ID of successfully saved items
                        if hasattr(single_item, 'id'):
                            successful_ids.add(single_item.id)
                    except Exception as individual_error:
                        skipped_log.write(f"{item.__name__} - ID: {getattr(single_item, 'id', 'unknown')} - SKIPPED: Individual save error: {str(individual_error)[:200]}\n")
                        skipped_count += 1
                        individual_fail_count += 1
                
                # Claude AI - Track IDs that were actually saved
                inserted_ids[item] = successful_ids
                items = [None] * success_count  # Just for counting

        # Claude AI - Build detailed skip message with breakdown
        msg = f"- Added **{len(items):,}** {item.__name__} objects."
        skip_details = []
        if fk_violation_count > 0:
            skip_details.append(f"{fk_violation_count} FK violations")
        if null_field_count > 0:
            skip_details.append(f"{null_field_count} null fields")
        if duplicate_count > 0:
            skip_details.append(f"{duplicate_count} duplicates")
        if validation_fail_count > 0:
            skip_details.append(f"{validation_fail_count} validation errors")
        
        if skip_details:
            msg += f" (skipped: {', '.join(skip_details)})"
        
        output[-1] = msg  # Claude AI - Replace progress message with final count
        skipped_log.write(f"\n{item.__name__} SUMMARY: Added {len(items):,}, Skipped {skipped_count}\n\n")
        await message.edit(embed=reload_embed())

    output.append("- Updating database sequences...")  # Claude AI - Progress message
    await message.edit(embed=reload_embed())
    
    await sequence_all_models()

    # Claude AI - Close the log files and copy to outputs
    skipped_log.write("\n=== END OF LOG ===\n")
    skipped_log.close()
    
    placeholder_log.write("\n=== END OF LOG ===\n")
    placeholder_log.write(f"\nTo find all placeholder players: SELECT * FROM player WHERE discord_id < -10000000000;\n")
    placeholder_log.write(f"To recover original player ID from placeholder: original_id = abs(discord_id + 10000000000)\n")
    placeholder_log.close()
    
    # Claude AI - Copy log files to outputs directory
    import shutil
    import os
    
    if os.path.exists("skipped_records.log"):
        shutil.copy("skipped_records.log", "/mnt/user-data/outputs/skipped_records.log")
    
    if os.path.exists("placeholder_assignments.log"):
        shutil.copy("placeholder_assignments.log", "/mnt/user-data/outputs/placeholder_assignments.log")
    
    output.append("- Migration complete! Logs saved: skipped_records.log, placeholder_assignments.log")

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
    """Clear all data from tables using TRUNCATE which also resets sequences."""
    # Claude AI - Changed from DELETE to TRUNCATE CASCADE for proper sequence reset
    client = Tortoise.get_connection("default")
    
    # Claude AI - Get all table names
    all_models = [
        Regime, Economy, Special, Ball, Player, GuildConfig, 
        Friendship, BlacklistedID, BlacklistedGuild, BallInstance, 
        Trade, TradeObject
    ]
    
    table_names = [model._meta.db_table for model in all_models]
    
    # Claude AI - TRUNCATE CASCADE will handle foreign key constraints and reset sequences
    if table_names:
        tables_str = ", ".join(table_names)
        try:
            await client.execute_query(f"TRUNCATE TABLE {tables_str} RESTART IDENTITY CASCADE;")
        except Exception as e:
            # Claude AI - If TRUNCATE fails, fall back to individual deletes and manual sequence reset
            output.append(f"- TRUNCATE failed, using fallback method: {str(e)}")
            for model in reversed(all_models):  # Reverse order for foreign keys
                await model.all().delete()
            
            # Claude AI - Manually reset sequences
            for model in all_models:
                try:
                    table = model._meta.db_table
                    await client.execute_query(f"ALTER SEQUENCE {table}_id_seq RESTART WITH 1;")
                except Exception:
                    pass  # Some tables might not have sequences


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

    output.append("- Clearing existing data...")  # Claude AI - Added progress message
    await message.edit(embed=reload_embed())  # Claude AI - Added progress update
    
    await clear_all_data()
    
    output.append("- Data cleared successfully. Starting migration...")  # Claude AI - Added progress message
    await message.edit(embed=reload_embed())  # Claude AI - Added progress update
    
    await load(message)


await main()  # type: ignore  # noqa: F704
