import asyncio
import bz2
import os
import shutil
import time
from datetime import datetime, date

import discord
from tortoise import Tortoise
from tortoise.fields.data import DatetimeField, DateField, FloatField, IntField
from tortoise.exceptions import ValidationError

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

__version__ = "1.0.3-cleaned"

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
    "S-EV": [Special, ["id", "background", "catch_phrase", "emoji", "end_date", "hidden", "name", "rarity", "start_date", "tradeable"]],
    "S-EX": [Special, ["id", "catch_phrase", "emoji", "background", "name", "rarity"]],
    "B": [Ball, ["id", "capacity_description", "capacity_name", "credits", "regime_id", "catch_names", "collection_card", "economy_id", "created_at", "emoji_id", "enabled", "country", "attack", "rarity", "short_name", "wild_card", "tradeable", "health"]],
    "BI": [BallInstance, ["id", "ball_id", "catch_date", "special_id", "favorite", "attack_bonus", "player_id", "server_id", "spawned_time", "trade_player_id", "tradeable", "health_bonus"]],
    "P": [Player, ["id", "discord_id", "donation_policy", "privacy_policy"]],
    "GC": [GuildConfig, ["id", "enabled", "guild_id", "spawn_channel"]],
    "F": [Friendship, ["id", "player1_id", "player2_id", "since"]],
    "BU": [BlacklistedID, ["id", "date", "discord_id", "reason"]],
    "BG": [BlacklistedGuild, ["id", "date", "discord_id", "reason"]],
    "T": [Trade, ["id", "date", "player1_id", "player2_id"]],
    "TO": [TradeObject, ["id", "ballinstance_id", "player_id", "trade_id"]],
}

def read_bz2(path: str):
    with bz2.open(path, "rb") as bz2f:
        return bz2f.read().splitlines()

output = []

def reload_embed(start_time: float | None = None, status="RUNNING"):
    embed = discord.Embed(title="BD-Migrator Process", description=f"Status: **{status}**")
    
    if status == "RUNNING":
        embed.color = discord.Color.yellow()
    elif status == "FINISHED":
        embed.color = discord.Color.green()
    elif status == "CANCELED":
        embed.color = discord.Color.red()

    if len(output) > 0:
        recent_output = output[-20:] if len(output) > 20 else output
        output_text = "\n".join(recent_output)
        if len(output_text) > 1000:
            output_text = "...\n" + output_text[-1000:]
        embed.add_field(name="Output", value=output_text)

    if start_time is not None:
        embed.set_footer(text=f"Ended migration in {round((time.time() - start_time), 3)}s")

    return embed


async def get_or_create_placeholder_player(missing_player_id, placeholder_log, created_placeholders):
    """Create a unique placeholder Player for a specific missing player ID."""
    placeholder_key = f"Player_{missing_player_id}"
    if placeholder_key in created_placeholders:
        return created_placeholders[placeholder_key]
    
    placeholder_discord_id = -10000000000 - missing_player_id
    placeholder_player = await Player.filter(discord_id=placeholder_discord_id).first()
    
    if not placeholder_player:
        placeholder_player = await Player.create(
            discord_id=placeholder_discord_id,
            donation_policy=0,
            privacy_policy=0
        )
        placeholder_log.write(f"Created placeholder Player (discord_id={placeholder_discord_id}, DB ID={placeholder_player.pk}) for missing Player ID {missing_player_id}\n")
    
    created_placeholders[placeholder_key] = placeholder_player.pk
    return placeholder_player.pk


async def load(message):
    lines = read_bz2("migration.txt.bz2")
    section = ""
    data = {}

    skipped_log = open("skipped_records.log", "w", encoding="utf-8")
    skipped_log.write("=== MIGRATION SKIPPED RECORDS LOG ===\n")
    skipped_log.write(f"Generated: {datetime.now()}\n\n")
    
    placeholder_log = open("placeholder_assignments.log", "w", encoding="utf-8")
    placeholder_log.write("=== PLACEHOLDER ASSIGNMENTS LOG ===\n")
    placeholder_log.write(f"Generated: {datetime.now()}\n")
    placeholder_log.write("Records assigned to placeholder entities:\n\n")
    
    created_placeholders = {}

    output.append(f"- Reading migration file with {len(lines):,} lines...")
    await message.edit(embed=reload_embed())

    for index, line in enumerate(lines, start=1):
        line = line.decode().rstrip()

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

            if value == "id" and line_data == "":
                skipped_log.write(f"Line {index} - {section_full[0].__name__}: SKIPPED - Empty ID field\n")
                model_dict = None
                break
            
            if line_data == "":
                continue

            if value not in fields:
                raise Exception(f"Unknown value '{value}' detected on line {index:,} - attribute {attribute_index:,} in {section_full[0].__name__} object")

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
                elif isinstance(field_type, DateField):
                    line_data = safe_date(line_data)

            if isinstance(line_data, str):
                line_data = line_data.replace("🮈", "\n")

            model_dict[value] = line_data

        if model_dict is not None:
            data[section_full[0]].append(model_dict)

    output.append(f"- Finished reading migration file. Processing {len(data)} model types...")
    await message.edit(embed=reload_embed())

    start_time = time.time()
    inserted_ids = {}
    
    for item, value in data.items():
        output.append(f"- Processing {item.__name__}... ({len(value):,} records to validate)")
        await message.edit(embed=reload_embed())
        
        fields_map = item._meta.fields_map
        
        # Identify foreign key fields
        fk_fields = {}
        for field_name, field_obj in fields_map.items():
            if hasattr(field_obj, 'related_model') and field_obj.related_model is not None:
                fk_fields[field_name] = field_obj.related_model
        
        seen_ids = set()
        unique_values = []
        skipped_count = 0
        fk_violation_count = 0
        null_field_count = 0
        duplicate_count = 0
        
        for idx, model in enumerate(value):
            if idx > 0 and idx % 5000 == 0:
                output[-1] = f"- Processing {item.__name__}... (validated {idx:,}/{len(value):,})"
                await message.edit(embed=reload_embed())
            
            model_id = model.get('id')
            
            if model_id is None:
                skipped_log.write(f"{item.__name__} - ID: None - SKIPPED: Null ID\n")
                skipped_count += 1
                continue
            
            if model_id in seen_ids:
                skipped_log.write(f"{item.__name__} - ID: {model_id} - SKIPPED: Duplicate ID\n")
                skipped_count += 1
                duplicate_count += 1
                continue
            
            # Validate foreign key references and create placeholders if needed
            has_invalid_fk = False
            for fk_field_name, related_model in fk_fields.items():
                fk_value = model.get(fk_field_name)
                if fk_value is not None:
                    exists_in_tracking = related_model in inserted_ids and fk_value in inserted_ids[related_model]
                    
                    if not exists_in_tracking:
                        exists_in_db = await related_model.filter(pk=fk_value).exists()
                        
                        if not exists_in_db:
                            if related_model == Player:
                                placeholder_id = await get_or_create_placeholder_player(fk_value, placeholder_log, created_placeholders)
                                
                                if Player not in inserted_ids:
                                    inserted_ids[Player] = set()
                                inserted_ids[Player].add(placeholder_id)
                                
                                model[fk_field_name] = placeholder_id
                                placeholder_log.write(f"{item.__name__} ID {model_id}: Reassigned {fk_field_name} from missing Player ID {fk_value} to placeholder DB ID {placeholder_id}\n")
                            else:
                                skipped_log.write(f"{item.__name__} - ID: {model_id} - SKIPPED: Invalid FK {fk_field_name}={fk_value} (references non-existent {related_model.__name__})\n")
                                has_invalid_fk = True
                                fk_violation_count += 1
                                break
            
            if has_invalid_fk:
                skipped_count += 1
                continue
            
            # Check for None values in non-nullable fields and set defaults
            skip_record = False
            null_fields = []
            defaults_set = []
            
            for field_name, field_value in list(model.items()):
                if field_value is None and field_name in fields_map:
                    field_obj = fields_map[field_name]
                    if hasattr(field_obj, 'null') and not field_obj.null:
                        if field_name == 'country':
                            model[field_name] = 'Unknown'
                            defaults_set.append(f"{field_name}='Unknown'")
                        elif field_name == 'short_name':
                            model[field_name] = 'Unknown'
                            defaults_set.append(f"{field_name}='Unknown'")
                        elif field_name == 'enabled':
                            model[field_name] = True
                            defaults_set.append(f"{field_name}=True")
                        elif field_name == 'tradeable':
                            model[field_name] = True
                            defaults_set.append(f"{field_name}=True")
                        else:
                            null_fields.append(field_name)
                            skip_record = True
            
            if defaults_set:
                placeholder_log.write(f"{item.__name__} ID {model_id}: Set defaults: {', '.join(defaults_set)}\n")
            
            if skip_record:
                skipped_log.write(f"{item.__name__} - ID: {model_id} - SKIPPED: Null required fields without defaults: {', '.join(null_fields)}\n")
                skipped_count += 1
                null_field_count += 1
                continue
                
            seen_ids.add(model_id)
            unique_values.append(model)
        
        output[-1] = f"- Creating {item.__name__} instances... ({len(unique_values):,} valid records)"
        await message.edit(embed=reload_embed())
        
        # Create model instances
        items = []
        validation_fail_count = 0
        emoji_validation_count = 0
        
        for idx, model in enumerate(unique_values):
            if idx > 0 and idx % 5000 == 0:
                output[-1] = f"- Creating {item.__name__} instances... ({idx:,}/{len(unique_values):,})"
                await message.edit(embed=reload_embed())
            
            # CRITICAL: Set defaults for required fields if they're None or missing
            if model.get('short_name') is None:
                model['short_name'] = 'Unknown'
            if model.get('country') is None:
                model['country'] = 'Unknown'
            if model.get('enabled') is None:
                model['enabled'] = True
            if model.get('tradeable') is None:
                model['tradeable'] = True
            
            # Validate Discord ID fields (must be 17-19 chars long)  
            emoji_id = model.get('emoji_id')
            if emoji_id is not None:
                try:
                    emoji_id_int = int(emoji_id)
                    emoji_id_str = str(emoji_id_int)
                    if len(emoji_id_str) < 17 or len(emoji_id_str) > 19:
                        # FIX invalid emoji_id with a valid placeholder (don't skip!)
                        model['emoji_id'] = 1234567890123456789  # Valid 19-digit placeholder
                        placeholder_log.write(f"{item.__name__} ID {model.get('id')}: Fixed invalid emoji_id (was {emoji_id}, len={len(emoji_id_str)})\n")
                        defaults_set.append(f"emoji_id=placeholder")
                except (ValueError, TypeError):
                    # FIX non-numeric emoji_id
                    model['emoji_id'] = 1234567890123456789  # Valid 19-digit placeholder
                    placeholder_log.write(f"{item.__name__} ID {model.get('id')}: Fixed non-numeric emoji_id (was {emoji_id})\n")
                    defaults_set.append(f"emoji_id=placeholder")
            
            try:
                instance = item(**model)
                # Validate the instance BEFORE adding to items
                # This will catch custom validators like emoji_id length check
                try:
                    await instance.full_clean()
                except AttributeError:
                    # full_clean might not exist, try manual field validation
                    pass
                except ValidationError as ve:
                    skipped_log.write(f"{item.__name__} - ID: {model.get('id')} - SKIPPED: Instance validation error: {str(ve)[:200]}\n")
                    skipped_log.write(f"  emoji_id: {model.get('emoji_id')}\n")
                    skipped_count += 1
                    validation_fail_count += 1
                    continue
                
                items.append(instance)
            except (ValueError, ValidationError) as e:
                skipped_log.write(f"{item.__name__} - ID: {model.get('id')} - SKIPPED: Validation error: {str(e)[:200]}\n")
                skipped_log.write(f"  emoji_id in model: {model.get('emoji_id')} (type: {type(model.get('emoji_id'))})\n")
                skipped_count += 1
                validation_fail_count += 1
                continue
        
        if emoji_validation_count > 0:
            output.append(f"  Note: Skipped {emoji_validation_count} items due to invalid emoji_id")
            await message.edit(embed=reload_embed())

        output[-1] = f"- Saving {item.__name__} to database... ({len(items):,} objects)"
        await message.edit(embed=reload_embed())

        if items:
            # CRITICAL: Fix ALL instances with invalid data before bulk_create
            fixed_count = 0
            for idx, instance in enumerate(items):
                # Check emoji_id if it exists
                if hasattr(instance, 'emoji_id') and instance.emoji_id is not None:
                    emoji_str = str(instance.emoji_id)
                    if len(emoji_str) < 17 or len(emoji_str) > 19:
                        old_val = instance.emoji_id
                        instance.emoji_id = 1234567890123456789  # Valid placeholder
                        placeholder_log.write(f"{item.__name__} - ID: {getattr(instance, 'id', 'unknown')} (item #{idx}) - Fixed emoji_id={old_val} (len={len(emoji_str)})\n")
                        fixed_count += 1
            
            # Log ALL emoji_ids to find the problem
            if item.__name__ == 'Ball':
                placeholder_log.write(f"\n=== ALL BALL EMOJI_IDS ===\n")
                for idx, instance in enumerate(items):
                    eid = getattr(instance, 'emoji_id', None)
                    eid_len = len(str(eid)) if eid is not None else 0
                    placeholder_log.write(f"Ball ID {getattr(instance, 'id', '?')} (item #{idx}): emoji_id={eid} (len={eid_len})\n")
                placeholder_log.write(f"=== END EMOJI_IDS ===\n\n")
            
            if fixed_count > 0:
                output.append(f"  Fixed {fixed_count} invalid emoji_ids")
                await message.edit(embed=reload_embed())
            
            try:
                await item.bulk_create(items)
                inserted_ids[item] = seen_ids
                
            except Exception as e:
                error_msg = f"ERROR: {type(e).__name__}: {str(e)[:500]}"
                skipped_log.write(f"\n{item.__name__} BULK CREATE FAILED: {error_msg}\n")
                skipped_log.write(f"First 3 items:\n")
                for i, failed_item in enumerate(items[:3]):
                    skipped_log.write(f"  Item {i}: {failed_item.__dict__}\n")
                
                output.append(f"- CRITICAL ERROR: Bulk create failed for {item.__name__}: {error_msg}")
                output.append(f"- Check skipped_records.log for details.")
                await message.edit(embed=reload_embed())
                
                skipped_log.close()
                placeholder_log.close()
                raise

        # Build detailed skip message
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
        
        output[-1] = msg
        skipped_log.write(f"\n{item.__name__} SUMMARY: Added {len(items):,}, Skipped {skipped_count}\n\n")
        await message.edit(embed=reload_embed())

    output.append("- Updating database sequences...")
    await message.edit(embed=reload_embed())
    
    await sequence_all_models()

    skipped_log.write("\n=== END OF LOG ===\n")
    skipped_log.close()
    
    placeholder_log.write("\n=== END OF LOG ===\n")
    placeholder_log.write(f"\nTo find all placeholder players: SELECT * FROM player WHERE discord_id < -10000000000;\n")
    placeholder_log.write(f"To recover original player ID: original_id = abs(discord_id + 10000000000)\n")
    placeholder_log.close()
    
    if os.path.exists("skipped_records.log"):
        shutil.copy("skipped_records.log", "/mnt/user-data/outputs/skipped_records.log")
    if os.path.exists("placeholder_assignments.log"):
        shutil.copy("placeholder_assignments.log", "/mnt/user-data/outputs/placeholder_assignments.log")
    
    output.append("- Migration complete! Logs saved.")

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


async def clear_all_data():
    """Clear all data from tables using TRUNCATE which also resets sequences."""
    client = Tortoise.get_connection("default")
    
    all_models = [Regime, Economy, Special, Ball, Player, GuildConfig, Friendship, BlacklistedID, BlacklistedGuild, BallInstance, Trade, TradeObject]
    table_names = [model._meta.db_table for model in all_models]
    
    if table_names:
        tables_str = ", ".join(table_names)
        try:
            await client.execute_query(f"TRUNCATE TABLE {tables_str} RESTART IDENTITY CASCADE;")
        except Exception as e:
            output.append(f"- TRUNCATE failed, using fallback: {str(e)}")
            for model in reversed(all_models):
                await model.all().delete()
            for model in all_models:
                try:
                    table = model._meta.db_table
                    await client.execute_query(f"ALTER SEQUENCE {table}_id_seq RESTART WITH 1;")
                except Exception:
                    pass


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

    output.append("- Clearing existing data...")
    await message.edit(embed=reload_embed())
    
    await clear_all_data()
    
    output.append("- Data cleared successfully. Starting migration...")
    await message.edit(embed=reload_embed())
    
    await load(message)


await main()  # type: ignore  # noqa: F704
